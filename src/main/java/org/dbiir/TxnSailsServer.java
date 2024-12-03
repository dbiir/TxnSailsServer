package org.dbiir;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LineBasedFrameDecoder;
import io.netty.handler.codec.string.StringDecoder;
import org.dbiir.worker.OfflineWorker;
import org.dbiir.worker.OnlineWorker;

import java.nio.charset.StandardCharsets;

public class TxnSailsServer {
    public static void main(String[] args) throws InterruptedException {
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        public void initChannel(SocketChannel ch) throws Exception {
                            ch.pipeline().addLast(new LineBasedFrameDecoder(1024));
                            ch.pipeline().addLast(new StringDecoder());
                            ch.pipeline().addLast(new ServerHandler());
                        }
                    });

            ChannelFuture f = b.bind(9876).sync();
            f.channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
        }
    }

    private static class ServerHandler extends ChannelInboundHandlerAdapter {
        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            String message = (String) msg;
            String[] parts = message.split(" ");
            String functionName = parts[0].toLowerCase(); // function name is case-insensitive
            String[] args = new String[parts.length - 1];
            System.arraycopy(parts, 1, args, 0, parts.length - 1);

            String response;
            switch (functionName) {
                case "execute":
                    response = OnlineWorker.getINSTANCE().execute(args);
                    break;
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
                    response = "OK," + idx;
                    break;
                case "register_end":
                    response = "OK";
                    OfflineWorker.getINSTANCE().register_end(args);
                    break;
                // add more function apis as needed
                default:
                    response = "Unknown function: " + functionName;
            }

            ByteBuf resp = ctx.alloc().buffer(response.length());
            resp.writeBytes(response.getBytes(StandardCharsets.UTF_8));
            ctx.writeAndFlush(resp);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            ctx.close();
        }
    }
}