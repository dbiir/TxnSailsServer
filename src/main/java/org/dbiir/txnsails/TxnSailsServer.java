package org.dbiir.txnsails;

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
import org.dbiir.txnsails.analysis.SchemaInfo;
import org.dbiir.txnsails.common.JacksonXmlConfiguration;
import org.dbiir.txnsails.common.types.CCType;
import org.dbiir.txnsails.common.types.DatabaseType;
import org.dbiir.txnsails.execution.WorkloadConfiguration;
import org.dbiir.txnsails.execution.utils.FileUtil;
import org.dbiir.txnsails.execution.validation.ValidationMetaTable;
import org.dbiir.txnsails.worker.Flusher;
import org.dbiir.txnsails.worker.MetaWorker;
import org.dbiir.txnsails.worker.OfflineWorker;
import org.dbiir.txnsails.worker.OnlineWorker;


import com.fasterxml.jackson.dataformat.xml.XmlMapper;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
public class TxnSailsServer {
  private static int DEFAULT_AUXILIARY_THREAD_NUM = 16; // used for
  private static Thread flushThread;
  private static ChannelFuture f;

  public static void main(String[] args)
      throws InterruptedException, ParseException, SQLException, IOException {
    CommandLineParser parser = new DefaultParser();
    Options options =
        new Options()
            .addOption("c", "config", true, "[required] Workload configuration file")
            .addOption("d", "directory", true, "Base directory for the meta files")
                .addOption("s", "schema", true, "Base directory for the schema sql files")
                .addOption("p", "phase", true, "Online predict or offline training");

    CommandLine argsLine = parser.parse(options, args);

    String schemaFile = argsLine.getOptionValue("s").trim();
    MetaWorker.getINSTANCE().setSchema(new SchemaInfo(schemaFile));

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
      createFlushThread(argsLine, workloadConfiguration.getBenchmarkName(), workloadConfiguration.getConcurrencyControlType());
      System.out.println("Create Flush Thread");
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

      f = b.bind(9876).sync();
      f.channel().closeFuture().sync();
    } finally {
      finishFlushThread();
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

  private static void createFlushThread(CommandLine argsLine, String benchmark, CCType ccType) {
    String metaDirectory = "metas";
    boolean onlinePredict = false;
    if (argsLine.hasOption("d")) {
      metaDirectory = argsLine.getOptionValue("d").trim();
      if (metaDirectory.contains("results")) {
        metaDirectory = metaDirectory.replace("results", "metas");
        int lastIndex = metaDirectory.lastIndexOf("_");

        if (lastIndex != -1) {
          metaDirectory = metaDirectory.substring(0, lastIndex - 1);
          metaDirectory += "/";
        }
      }
    }

    if (argsLine.hasOption("p")) {
      if (argsLine.getOptionValue("p").contains("online")) {
        onlinePredict = true;
      }
    }

    FileUtil.makeDirIfNotExists(metaDirectory);
    flushThread = new Thread(new Flusher(benchmark, metaDirectory, ccType,onlinePredict));
    flushThread.start();
  }

  private static void finishFlushThread() {
    flushThread.interrupt();
  }

  private static class ServerHandler extends ChannelInboundHandlerAdapter {
    private final ThreadLocal<OnlineWorker>
        onlineWorker; // online worker per thread, responsible for execution
    private static final String ERROR_FORMATTER =
        "ERROR#{0}#{1}#{2}"; // reason, SQLState, vendorCode

    private ServerHandler(WorkloadConfiguration configuration, int id) {
      this.onlineWorker = ThreadLocal.withInitial(() -> new OnlineWorker(configuration, id));
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws InterruptedException {
      String message = (String) msg;
//      System.out.println("Received: " + message);
      String[] parts = message.split("#");
      for (int i = 0; i < parts.length; i++) {
        parts[i] = parts[i].trim();
      }
      String functionName = parts[0].toLowerCase(); // function name is case-insensitive
      String[] args = new String[parts.length - 1];
      System.arraycopy(parts, 1, args, 0, parts.length - 1);

      String response;
      switch (functionName) {
        case "execute" -> {
          try {
            response = onlineWorker.get().execute(args);
            response = "OK#" + response;
//            System.out.println("response: " + response);
          } catch (SQLException ex) {
            // wrap error message
            response = MessageFormat.format(ERROR_FORMATTER, ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
          }
        }
        case "commit" -> {
          try {
            onlineWorker.get().commit();
            response = "OK";
          } catch (SQLException ex) {
            // wrap error message
            response = MessageFormat.format(
                    ERROR_FORMATTER, ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
          }
        }
        case "rollback" -> {
          try {
            onlineWorker.get().rollback();
            response = "OK";
          } catch (SQLException ex) {
            // wrap error message
            response = MessageFormat.format(
                    ERROR_FORMATTER, ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
          }
        }
        case "register_begin" -> {
          response = "OK";
          OfflineWorker.getINSTANCE().register_begin(args);
        }
        case "register" -> {
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
        }
        case "register_end", "analysis" -> {
          response = "OK";
          OfflineWorker.getINSTANCE().register_end(args);
        }
        case "close" -> {
          ctx.channel().close();
          f.channel().close();
          return;
        }
        default -> response = "Unknown function: " + functionName;
      }
      // control command
      ByteBuf resp = ctx.alloc().buffer(response.length());
      resp.writeBytes(response.getBytes(StandardCharsets.UTF_8));
      ctx.writeAndFlush(resp).sync();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      cause.printStackTrace();
      ctx.close();
    }
  }
}
