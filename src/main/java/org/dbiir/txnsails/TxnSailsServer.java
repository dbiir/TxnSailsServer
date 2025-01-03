package org.dbiir.txnsails;

import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;
import org.dbiir.txnsails.common.DatabaseType;
import org.dbiir.txnsails.common.JacksonXmlConfiguration;
import org.dbiir.txnsails.execution.WorkloadConfiguration;
import org.dbiir.txnsails.execution.validation.ValidationMetaTable;
import org.dbiir.txnsails.worker.OfflineWorker;
import org.dbiir.txnsails.worker.OnlineWorker;

public class TxnSailsServer {
  private static int DEFAULT_AUXILIARY_THREAD_NUM = 16; // used for

  public static void main(String[] args)
      throws InterruptedException, ParseException, SQLException, IOException {
    CommandLineParser parser = new DefaultParser();
    Options options =
        new Options()
            .addOption("c", "config", true, "[required] Workload configuration file")
            .addOption("p", "phase", true, "Online predict or offline training");

    CommandLine argsLine = parser.parse(options, args);

    String configFile = argsLine.getOptionValue("c").trim();

    JacksonXmlConfiguration xmlConfig = buildConfiguration(configFile);
    AtomicInteger genWorkerId = new AtomicInteger(0);
    WorkloadConfiguration workloadConfiguration = loadConfiguration(xmlConfig);
    List<Connection> auxiliaryConnectionList = makeAuxiliaryConnections(workloadConfiguration);
    ValidationMetaTable.getInstance()
        .initHotspot(workloadConfiguration.getBenchmarkName(), auxiliaryConnectionList);

    EventLoopGroup bossGroup = new NioEventLoopGroup();
    EventLoopGroup workerGroup = new NioEventLoopGroup();
    try {
      ServerBootstrap b = new ServerBootstrap();
      b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .childHandler(
              new ChannelInitializer<SocketChannel>() {
                @Override
                public void initChannel(SocketChannel ch) throws Exception {
                  ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(4096, 0, 4, 0, 4));
                  // Encoder
                  ch.pipeline().addLast(new LengthFieldPrepender(4));
                  ch.pipeline().addLast(new StringDecoder());
                  ch.pipeline().addLast(new StringEncoder());
                  ch.pipeline()
                      .addLast(new ServerHandler(workloadConfiguration, genWorkerId.addAndGet(1)));
                }
              });

      ChannelFuture f = b.bind(9876).sync();
      f.channel().closeFuture().sync();
    } finally {
      workerGroup.shutdownGracefully();
      bossGroup.shutdownGracefully();
    }
  }

  public static JacksonXmlConfiguration buildConfiguration(String filename) throws IOException {
    //    String currentPath = System.getProperty("user.dir");
    //    System.out.println("Current working directory: " + currentPath);
    //    System.out.println("Loading configuration from " + filename);

    XmlMapper xmlMapper = new XmlMapper();
    return xmlMapper.readValue(new File(filename), JacksonXmlConfiguration.class);
  }

  private static WorkloadConfiguration loadConfiguration(JacksonXmlConfiguration xmlConfig) {
    // ----------------------------------------------------------------
    // BEGIN LOADING BENCHMARK CONFIGURATION
    // Simplify the evaluation, the config file is provided by application, TxnSails would provide
    // an api in future
    // ----------------------------------------------------------------

    WorkloadConfiguration wrkld = new WorkloadConfiguration();
    wrkld.setXmlConfig(xmlConfig);

    // Pull in database configuration
    wrkld.setDatabaseType(DatabaseType.get(xmlConfig.getType()));
    wrkld.setDriverClass(xmlConfig.getDriver());
    wrkld.setUrl(xmlConfig.getUrl());
    wrkld.setUsername(xmlConfig.getUsername());
    wrkld.setPassword(xmlConfig.getPassword());
    wrkld.setRandomSeed(xmlConfig.getRandomSeed());
    wrkld.setBatchSize(xmlConfig.getBatchsize());
    wrkld.setMaxRetries(xmlConfig.getMaxRetries());
    wrkld.setNewConnectionPerTxn(xmlConfig.isNewConnectionPerTxn());
    wrkld.setReconnectOnConnectionFailure(xmlConfig.isReconnectOnConnectionFailure());
    wrkld.setBenchmarkName(xmlConfig.getBenchmark().toLowerCase());

    int terminals = xmlConfig.getTerminals();
    wrkld.setTerminals(terminals);

    wrkld.setScaleFactor(xmlConfig.getScalefactor());
    String type = xmlConfig.getConcurrencyControlType();
    wrkld.setConcurrencyControlType(type);

    return wrkld;
  }

  private static List<Connection> makeAuxiliaryConnections(WorkloadConfiguration workConf)
      throws SQLException {
    List<Connection> connectionList = new ArrayList<>(16);
    for (int i = 0; i < DEFAULT_AUXILIARY_THREAD_NUM; i++) {
      if (StringUtils.isEmpty(workConf.getUsername())) {
        connectionList.add(DriverManager.getConnection(workConf.getUrl()));
      } else {
        connectionList.add(
            DriverManager.getConnection(
                workConf.getUrl(), workConf.getUsername(), workConf.getPassword()));
      }
    }
    return connectionList;
  }

  private static class ServerHandler extends ChannelInboundHandlerAdapter {
    private final ThreadLocal<OnlineWorker>
        onlineWorker; // online worker per thread, responsible for execution
    private static final String ERROR_FORMATTER =
        "ERROR#{0}#{1}#{2}"; // reason, SQLState, vendorCode

    private ServerHandler(WorkloadConfiguration configuration, int id) {
      this.onlineWorker = new ThreadLocal<>();
      this.onlineWorker.set(new OnlineWorker(configuration, id));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws InterruptedException {
      String message = (String) msg;
      System.out.println("Received: " + message);
      String[] parts = message.split("#");
      for (int i = 0; i < parts.length; i++) {
        parts[i] = parts[i].trim();
      }
      String functionName = parts[0].toLowerCase(); // function name is case-insensitive
      String[] args = new String[parts.length - 1];
      System.arraycopy(parts, 1, args, 0, parts.length - 1);

      String response;
      switch (functionName) {
        case "execute":
          try {
            response = onlineWorker.get().execute(args);
          } catch (SQLException ex) {
            // wrap error message
            response =
                MessageFormat.format(
                    ERROR_FORMATTER, ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
          }
          break;
        // control command
        case "commit":
          try {
            onlineWorker.get().commit();
            response = "OK";
          } catch (SQLException ex) {
            // wrap error message
            response =
                MessageFormat.format(
                    ERROR_FORMATTER, ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
          }
          break;
        case "rollback":
          try {
            onlineWorker.get().rollback();
            response = "OK";
          } catch (SQLException ex) {
            // wrap error message
            response =
                MessageFormat.format(
                    ERROR_FORMATTER, ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
          }
          break;
        // TODO: remove register_begin and register_end
        case "register_begin":
          response = "OK";
          OfflineWorker.getINSTANCE().register_begin(args);
          break;
        case "register":
          if (args.length < 4) {
            response = "FAILED";
            break;
          }
          int idx = OfflineWorker.getINSTANCE().register(args);
          if (idx < 0) {
            response = "FAILED";
            break;
          }
          response = "OK#" + idx; // response with the unique sql index in server-side
          break;
        case "register_end":
        case "analysis":
          response = "OK";
          OfflineWorker.getINSTANCE().register_end(args);
          break;
        // add more function apis as needed
        default:
          response = "Unknown function: " + functionName;
      }
      System.out.println(response);
      ByteBuf resp = ctx.alloc().buffer(response.length());
      resp.writeBytes(response.getBytes(StandardCharsets.UTF_8));
      ctx.writeAndFlush(resp).sync();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      ctx.close();
    }
  }
}
