package com.github.fmjsjx.conveyor;

import java.util.Arrays;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.github.fmjsjx.conveyor.admin.AdminClient;
import com.github.fmjsjx.conveyor.util.BannerUtil;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@SpringBootApplication
public class ConveyorSetsApplication {

    public static void main(String[] args) throws Exception {
        if (args.length > 0 && "admin".equals(args[0])) {
            var argments = Arrays.copyOfRange(args, 1, args.length);
            AdminClient.main(argments);
            return;
        }
        var ctx = SpringApplication.run(ConveyorSetsApplication.class, args);
        var app = ctx.getBean(AppProperties.class);
        BannerUtil.printGameBanner(s -> log.info("-- Banner --\n{}", s), app.getName(), app.getVersion());
    }

}
