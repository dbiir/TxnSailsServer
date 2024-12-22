package org.dbiir.txnsails;
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
import org.apache.commons.configuration2.XMLConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.convert.DisabledListDelimiterHandler;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.tree.xpath.XPathExpressionEngine;
import org.apache.commons.lang3.StringUtils;
import org.dbiir.txnsails.execution.WorkloadConfiguration;
import org.dbiir.txnsails.common.DatabaseType;
import org.dbiir.txnsails.execution.validation.LockTable;
import org.dbiir.txnsails.worker.OfflineWorker;
import org.dbiir.txnsails.worker.OnlineWorker;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import java.lang.constant.Constable;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Formatter;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class TxnSailsServer {
    private static int DEFAULT_AUXILIARY_THREAD_NUM = 16; // used for

    public static void main(String[] args) throws InterruptedException, ParseException, ConfigurationException, SQLException {
        CommandLineParser parser = new DefaultParser();
        Options options = new Options()
                .addOption("c", "config", true, "[required] Workload configuration file")
                .addOption("p", "phase", true, "Online predict or offline training");

        CommandLine argsLine = parser.parse(options, args);

        String configFile = argsLine.getOptionValue("c").trim();

        XMLConfiguration xmlConfig = buildConfiguration(configFile);
        AtomicInteger genWorkerId = new AtomicInteger(0);
        WorkloadConfiguration workloadConfiguration = loadConfiguration(xmlConfig);
        List<Connection> auxiliaryConnectionList = makeAuxiliaryConnections(workloadConfiguration);
        LockTable.getInstance().initHotspot(workloadConfiguration.getBenchmarkName(), auxiliaryConnectionList);

        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new LengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4));
                            // Encoder
                            ch.pipeline().addLast(new LengthFieldPrepender(4));
                            ch.pipeline().addLast(new StringDecoder());
                            ch.pipeline().addLast(new StringEncoder());
                            ch.pipeline().addLast(new ServerHandler(workloadConfiguration, genWorkerId.addAndGet(1)));
                        }
                    });

            ChannelFuture f = b.bind(9876).sync();
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    public static XMLConfiguration buildConfiguration(String filename) throws ConfigurationException {
        Parameters params = new Parameters();
        FileBasedConfigurationBuilder<XMLConfiguration> builder =
                new FileBasedConfigurationBuilder<>(XMLConfiguration.class)
                        .configure(
                                params
                                        .xml()
                                        .setFileName(filename)
                                        .setListDelimiterHandler(new DisabledListDelimiterHandler())
                                        .setExpressionEngine(new XPathExpressionEngine()));
        return builder.getConfiguration();
    }

    private static WorkloadConfiguration loadConfiguration(XMLConfiguration xmlConfig) throws ParseException {
        int lastTxnId = 0;
        // ----------------------------------------------------------------
        // BEGIN LOADING BENCHMARK CONFIGURATION
        // Simplify the evaluation, the config file is provided by application, TxnSails would provide an api in future
        // ----------------------------------------------------------------

        WorkloadConfiguration wrkld = new WorkloadConfiguration();
        wrkld.setXmlConfig(xmlConfig);

        // Pull in database configuration
        wrkld.setDatabaseType(DatabaseType.get(xmlConfig.getString("type")));
        wrkld.setDriverClass(xmlConfig.getString("driver"));
        wrkld.setUrl(xmlConfig.getString("url"));
        wrkld.setUsername(xmlConfig.getString("username"));
        wrkld.setPassword(xmlConfig.getString("password"));
        wrkld.setRandomSeed(xmlConfig.getInt("randomSeed", -1));
        wrkld.setBatchSize(xmlConfig.getInt("batchsize", 128));
        wrkld.setMaxRetries(xmlConfig.getInt("retries", 3));
        wrkld.setNewConnectionPerTxn(xmlConfig.getBoolean("newConnectionPerTxn", false));
        wrkld.setReconnectOnConnectionFailure(
                xmlConfig.getBoolean("reconnectOnConnectionFailure", true));
        //
        wrkld.setBenchmarkName(xmlConfig.getString("benchmark", "ycsb").toLowerCase());

        int terminals = xmlConfig.getInt("terminals", 0);
        wrkld.setTerminals(terminals);

        String isolationMode =
                xmlConfig.getString("benchmark[not(@bench)]", "TRANSACTION_SERIALIZABLE");
        wrkld.setScaleFactor(xmlConfig.getDouble("scalefactor", 1.0));
        String type = xmlConfig.getString("concurrencyControlType", "SERIALIZABLE");
        wrkld.setConcurrencyControlType(type);

        return wrkld;
    }

    private static List<Connection> makeAuxiliaryConnections(WorkloadConfiguration workConf) throws SQLException {
        List<Connection> connectionList = new ArrayList<>(16);
        for (int i = 0; i < DEFAULT_AUXILIARY_THREAD_NUM; i++) {
            if (StringUtils.isEmpty(workConf.getUsername())) {
                connectionList.add(DriverManager.getConnection(workConf.getUrl()));
            } else {
                connectionList.add(DriverManager.getConnection(workConf.getUrl(), workConf.getUsername(), workConf.getPassword()));
            }
        }
        return connectionList;
    }


    private static class ServerHandler extends ChannelInboundHandlerAdapter {
        private final ThreadLocal<OnlineWorker> onlineWorker; // online worker per thread, responsible for execution
        private static final String ERROR_FORMATTER = "ERROR#{0}#{1}#{2}"; // reason, SQLState, vendorCode

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
                        response = MessageFormat.format(ERROR_FORMATTER, ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
                    }
                    break;
                // control command
                case "commit":
                    try {
                        onlineWorker.get().commit();
                        response = "OK";
                    } catch (SQLException ex) {
                        // wrap error message
                        response = MessageFormat.format(ERROR_FORMATTER, ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
                    }
                    break;
                case "rollback":
                    try {
                        onlineWorker.get().rollback();
                        response = "OK";
                    } catch (SQLException ex) {
                        // wrap error message
                        response = MessageFormat.format(ERROR_FORMATTER, ex.getMessage(), ex.getSQLState(), ex.getErrorCode());
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