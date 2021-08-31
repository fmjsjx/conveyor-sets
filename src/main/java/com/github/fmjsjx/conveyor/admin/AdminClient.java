package com.github.fmjsjx.conveyor.admin;

import com.github.fmjsjx.conveyor.config.ConveyorSetsConfig;
import com.github.fmjsjx.conveyor.util.ConfigUtil;

import io.netty.channel.epoll.Epoll;

public class AdminClient {

    public static void main(String[] args) throws Exception {
        var mainCfg = ConfigUtil.loadConfiguration("conveyor-sets.yml", ConveyorSetsConfig::loadFromYaml);
        if (mainCfg.unixServer().isPresent() && Epoll.isAvailable()) {
            var client = new UnixClient(mainCfg.unixServer().get());
            client.command(args);
            return;
        }
        if (mainCfg.resp3Server().isPresent()) {
            var client = new Resp3Client(mainCfg.resp3Server().get());
            client.command(args);
            return;
        }
        throw new IllegalStateException("none admin server on conveyor-sets.yml");
    }

}
