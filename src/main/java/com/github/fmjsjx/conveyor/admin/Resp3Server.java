package com.github.fmjsjx.conveyor.admin;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import com.github.fmjsjx.conveyor.config.Resp3ServerConfig;
import com.github.fmjsjx.conveyor.service.ConveyorSetManager;
import com.github.fmjsjx.libnetty.resp.CachedBulkStringMessage;
import com.github.fmjsjx.libnetty.resp.CachedErrorMessage;
import com.github.fmjsjx.libnetty.resp.DefaultArrayMessage;
import com.github.fmjsjx.libnetty.resp.RedisRequest;
import com.github.fmjsjx.libnetty.resp.RedisRequestDecoder;
import com.github.fmjsjx.libnetty.resp.RespBulkStringMessage;
import com.github.fmjsjx.libnetty.resp.RespMessage;
import com.github.fmjsjx.libnetty.resp.RespMessageEncoder;
import com.github.fmjsjx.libnetty.resp.RespMessages;
import com.github.fmjsjx.libnetty.resp3.DefaultMapMessage;
import com.github.fmjsjx.libnetty.resp3.Resp3Messages;
import com.github.fmjsjx.libnetty.transport.TransportLibrary;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.util.concurrent.DefaultThreadFactory;
import io.netty.util.concurrent.Future;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Resp3Server implements AdminServer {

    static final RespMessageEncoder respMessageEncoder = new RespMessageEncoder();

    static final CachedErrorMessage NOAUTH_HELLO = CachedErrorMessage.createAscii(
            "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time");
    private static final CachedErrorMessage PROTO_ERROR = CachedErrorMessage
            .createAscii("NOPROTO sorry this protocol version is not supported");

    static final CachedBulkStringMessage SERVER_KEY = CachedBulkStringMessage.createAscii("server");
    static final CachedBulkStringMessage SERVER_VALUE = CachedBulkStringMessage.createAscii("conveyor-sets");
    static final CachedBulkStringMessage VERSION_KEY = CachedBulkStringMessage.createAscii("version");
    static final CachedBulkStringMessage PROTO_KEY = CachedBulkStringMessage.createAscii("proto");
    static final CachedBulkStringMessage ID_KEY = CachedBulkStringMessage.createAscii("id");
    static final CachedBulkStringMessage MODE_KEY = CachedBulkStringMessage.createAscii("mode");
    static final CachedBulkStringMessage MODE_VALUE = CachedBulkStringMessage.createAscii("standalone");
    static final CachedBulkStringMessage ROLE_KEY = CachedBulkStringMessage.createAscii("role");
    static final CachedBulkStringMessage ROLE_VALUE = CachedBulkStringMessage.createAscii("master");
    static final CachedBulkStringMessage MODULES_KEY = CachedBulkStringMessage.createAscii("modules");

    private final ConveyorSetManager conveyorSetManager;
    private final EventLoopGroup group;
    final CachedBulkStringMessage versionValue;
    final AtomicLong clientIdentity = new AtomicLong();

    public Resp3Server(String version, ConveyorSetManager conveyorSetManager, Resp3ServerConfig config) {
        this.versionValue = CachedBulkStringMessage.createAscii(version);
        this.conveyorSetManager = conveyorSetManager;
        var transportLibrary = TransportLibrary.getDefault();
        this.group = transportLibrary.createGroup(1, new DefaultThreadFactory("resp3"));
        var bootstrap = new ServerBootstrap().group(group).channel(transportLibrary.serverChannelClass())
                .childOption(ChannelOption.TCP_NODELAY, true).childHandler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) throws Exception {
                        ch.pipeline().addLast(respMessageEncoder).addLast(new RedisRequestDecoder())
                                .addLast(new Resp3ServerHandler(config));
                    }
                });
        try {
            var address = config.address().map(hostname -> new InetSocketAddress(hostname, config.port()))
                    .orElseGet(() -> new InetSocketAddress(config.port()));
            bootstrap.bind(address).sync();
            log.info("[admin:startup] Resp3 server started at {}", address);
        } catch (Exception e) {
            log.warn("[admin:startup] Resp3 server start up failed", e);
            this.group.shutdownGracefully();
        }
    }

    @Override
    public Future<?> shutdown() {
        var group = this.group;
        if (group.isShuttingDown()) {
            return group.terminationFuture();
        }
        log.info("[admin:shutdown] Shutdown resp3 server");
        return group.shutdownGracefully();
    }

    class Resp3ServerHandler extends SimpleChannelInboundHandler<RedisRequest> {

        boolean verified = true;
        BiPredicate<String, String> authPredicate;
        final long id;

        Resp3ServerHandler(Resp3ServerConfig config) {
            this.id = clientIdentity.incrementAndGet();
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
        protected void channelRead0(ChannelHandlerContext ctx, RedisRequest msg) throws Exception {
            var cmd = msg.command().toText().toLowerCase();
            switch (cmd) {
            case "hello":
                hello(ctx, msg);
                break;
            case "quit":
                ctx.writeAndFlush(RespMessages.ok()).addListener(ChannelFutureListener.CLOSE);
                break;
            case "auth":
                if (!verified) {
                    if (msg.size() > 3) {
                        ctx.writeAndFlush(RespMessages.error("syntax error"));
                    }
                    var username = "default";
                    var password = msg.argument(1).textValue();
                    if (msg.size() == 3) {
                        username = password;
                        password = msg.argument(2).textValue();
                    }
                    if (authPredicate.test(username, password)) {
                        verified = true;
                        ctx.writeAndFlush(RespMessages.ok());
                    } else {
                        ctx.writeAndFlush(Resp3Messages.wrongPass());
                    }
                } else {
                    ctx.writeAndFlush(RespMessages.ok());
                }
                break;
            case "ping":
                if (verified) {
                    if (msg.size() == 1) {
                        ctx.writeAndFlush(RespMessages.pong());
                    } else if (msg.size() == 2) {
                        ctx.writeAndFlush(msg.argument(1).retainedDuplicate());
                    } else {
                        ctx.writeAndFlush(RespMessages.wrongNumberOfArgumentsForCommand(cmd));
                    }
                } else {
                    ctx.writeAndFlush(RespMessages.noauth());
                }
                break;
            case "select":
                if (verified) {
                    ctx.writeAndFlush(RespMessages.ok());
                } else {
                    ctx.writeAndFlush(RespMessages.noauth());
                }
                break;
            case "get":
                if (verified) {
                    if (msg.size() > 1) {
                        var key = msg.argument(1).toText();
                        var value = conveyorSetManager.adminCommandLine(key);
                        ctx.writeAndFlush(RespMessages.bulkString(value));
                    } else {
                        ctx.writeAndFlush(RespMessages.wrongNumberOfArgumentsForCommand(cmd));
                    }
                } else {
                    ctx.writeAndFlush(RespMessages.noauth());
                }
                break;
            default:
                if (msg.size() == 1) {
                    ctx.writeAndFlush(RespMessages.error("unknown command `" + cmd + "`"));
                } else {
                    var args = msg.arguments(1).map(a -> "`" + a + "`").collect(Collectors.joining(", "));
                    var err = RespMessages.error("unknown command `" + cmd + "`, with args beginning with: " + args);
                    ctx.writeAndFlush(err);
                }
                break;
            }
        }

        private void hello(ChannelHandlerContext ctx, RedisRequest msg) {
            var args = msg.arguments(2).map(RespBulkStringMessage::textValue).toList();
            var authIndex = -1;
            for (var i = 0; i < args.size(); i++) {
                var arg = args.get(i);
                if ("auth".equalsIgnoreCase(arg)) {
                    authIndex = i;
                    break;
                }
            }
            if (authIndex >= 0) {
                if (args.size() < authIndex + 2) {
                    ctx.writeAndFlush(RespMessages.wrongNumberOfArgumentsForCommand("hello"));
                } else {
                    if (!verified) {
                        var username = args.get(authIndex + 1);
                        var password = args.get(authIndex + 2);
                        verified = authPredicate.test(username, password);
                    }
                    if (!verified) {
                        ctx.writeAndFlush(Resp3Messages.wrongPass());
                    } else {
                        hello0(ctx, msg);
                    }
                }
            } else {
                if (verified) {
                    if (msg.size() == 1) {
                        ctx.writeAndFlush(version2HelloResult());
                    } else {
                        hello0(ctx, msg);
                    }
                } else {
                    ctx.writeAndFlush(NOAUTH_HELLO);
                }
            }
        }

        private void hello0(ChannelHandlerContext ctx, RedisRequest msg) {
            var protover = msg.argument(1).textValue();
            switch (protover) {
            case "2":
                ctx.writeAndFlush(version2HelloResult());
                break;
            case "3":
                ctx.writeAndFlush(version3HelloResult());
                break;
            default:
                ctx.writeAndFlush(PROTO_ERROR);
                break;
            }
        }

        private DefaultArrayMessage<RespMessage> version2HelloResult() {
            var array = new DefaultArrayMessage<>(SERVER_KEY, SERVER_VALUE, VERSION_KEY, versionValue, PROTO_KEY,
                    RespMessages.integer(2), ID_KEY, RespMessages.integer(id), MODE_KEY, MODE_VALUE, ROLE_KEY,
                    ROLE_VALUE, MODULES_KEY, RespMessages.emptyArray());
            return array;
        }

        private DefaultMapMessage<RespMessage, RespMessage> version3HelloResult() {
            var map = new DefaultMapMessage<>();
            map.put(SERVER_KEY, SERVER_VALUE);
            map.put(VERSION_KEY, versionValue);
            map.put(PROTO_KEY, RespMessages.integer(3));
            map.put(ID_KEY, RespMessages.integer(id));
            map.put(MODE_KEY, MODE_VALUE);
            map.put(ROLE_KEY, ROLE_VALUE);
            map.put(MODULES_KEY, RespMessages.emptyArray());
            return map;
        }

    }

}
