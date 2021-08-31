package com.github.fmjsjx.conveyor.admin;

import java.io.File;
import java.util.function.BiPredicate;

import com.github.fmjsjx.conveyor.config.UnixServerConfig;
import com.github.fmjsjx.conveyor.service.ConveyorSetManager;
import com.github.fmjsjx.libnetty.resp.RespMessages;
import com.github.fmjsjx.libnetty.resp3.Resp3Messages;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.codec.LengthFieldPrepender;
import io.netty.util.CharsetUtil;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class UnixServer implements AdminServer {

    private static final LengthFieldPrepender LENGTH_FIELD_PREPENDER_4 = new LengthFieldPrepender(4);

    private final ConveyorSetManager conveyorSetManager;
    private final EventLoopGroup group;

    public UnixServer(ConveyorSetManager conveyorSetManager, UnixServerConfig config) {
        this.conveyorSetManager = conveyorSetManager;
        this.group = new EpollEventLoopGroup(1, new DefaultThreadFactory("unix"));
        var bootstrap = new ServerBootstrap().group(group).channel(EpollServerDomainSocketChannel.class)
                .childOption(ChannelOption.TCP_NODELAY, true).childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.pipeline().addLast(LENGTH_FIELD_PREPENDER_4)
                                .addLast(new LengthFieldBasedFrameDecoder(1024, 0, 4, 0, 4))
                                .addLast(new UnixServerHandler(config));
                    }
                });
        var file = new File(config.file());
        try {
            if (!file.exists()) {
                var dir = file.getParentFile();
                if (!dir.exists()) {
                    dir.mkdirs();
                }
            }
            bootstrap.bind(new DomainSocketAddress(file)).sync();
            log.info("[admin:startup] Unix server started at {}", file);
        } catch (Exception e) {
            log.warn("[admin:startup] Unix server at {} start up failed", file, e);
            this.group.shutdownGracefully();
        }
    }

    @Override
    public Future<?> shutdown() {
        var group = this.group;
        if (group.isShutdown()) {
            return group.terminationFuture();
        }
        log.info("[admin:shutdown] Shutdown unix server");
        return group.shutdownGracefully();
    }

    class UnixServerHandler extends SimpleChannelInboundHandler<ByteBuf> {

        boolean verified = true;
        BiPredicate<String, String> authPredicate;

        UnixServerHandler(UnixServerConfig config) {
            config.password().ifPresent(password -> {
                verified = false;
                var username = config.username().orElse("default");
                authPredicate = (u, p) -> username.equals(u) && password.equals(p);
            });
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            ctx.close();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
            var text = msg.toString(CharsetUtil.UTF_8);
            if (text.startsWith("auth ")) {
                var value = "OK";
                if (!verified) {
                    var args = text.split("\\s+", 3);
                    if (args.length < 3) {
                        value = RespMessages.wrongNumberOfArgumentsForCommand("auth").text();
                    } else {
                        var username = args[1];
                        var password = args[2];
                        if (authPredicate.test(username, password)) {
                            verified = true;
                        } else {
                            value = Resp3Messages.wrongPass().text();
                        }
                    }
                }
                var buf = ctx.alloc().buffer();
                buf.writeBytes(value.getBytes(CharsetUtil.UTF_8));
                ctx.writeAndFlush(buf);
            } else if (text.startsWith("quit ") || text.startsWith("exit ")) {
                var buf = ctx.alloc().buffer();
                buf.writeBytes("OK".getBytes(CharsetUtil.UTF_8));
                ctx.writeAndFlush(buf).addListener(ChannelFutureListener.CLOSE);
            } else {
                var value = verified ? conveyorSetManager.adminCommandLine(text) : RespMessages.noauth().text();
                var buf = ctx.alloc().buffer();
                buf.writeBytes(value.getBytes(CharsetUtil.UTF_8));
                ctx.writeAndFlush(buf);
            }
        }

    }

}
