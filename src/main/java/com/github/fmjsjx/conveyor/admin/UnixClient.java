package com.github.fmjsjx.conveyor.admin;

import java.util.concurrent.SynchronousQueue;

import com.github.fmjsjx.conveyor.config.UnixServerConfig;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultThreadFactory;

public class UnixClient {

    final UnixServerConfig config;

    public UnixClient(UnixServerConfig config) {
        this.config = config;
    }

    public void command(String[] args) throws Exception {
        var group = new EpollEventLoopGroup(1, new DefaultThreadFactory("unix-client"));
        try {
            var responseQueue = new SynchronousQueue<String>();
            var bootstrap = new Bootstrap().group(group).channel(EpollDomainSocketChannel.class)
                    .option(ChannelOption.TCP_NODELAY, true).handler(new ChannelInitializer<Channel>() {
                        @Override
                        protected void initChannel(Channel ch) throws Exception {
                            ch.pipeline().addLast(new ReadTimeoutHandler(180)).addLast(new LengthFieldPrepender(4))
                                    .addLast(new LengthFieldBasedFrameDecoder(1048576, 0, 4, 0, 4))
                                    .addLast(new UnixClientHandler(responseQueue, config.password().isEmpty()));
                        }
                    });
            var channel = bootstrap.connect(new DomainSocketAddress(config.file())).sync().channel();
            if (!channel.isActive()) {
                System.err.println("open unix domain channel on `" + config.file() + "` failed");
                throw new RuntimeException("channel is inactive");
            }
            if (config.password().isPresent()) {
                var username = config.username().orElse("default");
                var password = config.password().get();
                var line = "auth " + username + " " + password;
                channel.writeAndFlush(Unpooled.copiedBuffer(line, CharsetUtil.UTF_8));
            }
            channel.writeAndFlush(Unpooled.copiedBuffer(String.join(" ", args), CharsetUtil.UTF_8));
            var result = responseQueue.take();
            System.out.println(result);
        } finally {
            group.shutdownGracefully();
        }
    }

    static class UnixClientHandler extends SimpleChannelInboundHandler<ByteBuf> {

        private final SynchronousQueue<String> responseQueue;
        private boolean verified;

        public UnixClientHandler(SynchronousQueue<String> responseQueue, boolean verified) {
            this.responseQueue = responseQueue;
            this.verified = verified;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            System.err.println("ERROR unexpected error occurs");
            cause.printStackTrace();
            responseQueue.add("");
            ctx.close();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            if (verified) {
                responseQueue.add(msg.toString(CharsetUtil.UTF_8));
            }
        }

    }

}
