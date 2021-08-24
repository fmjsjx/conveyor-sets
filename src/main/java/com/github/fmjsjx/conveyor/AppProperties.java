package com.github.fmjsjx.conveyor;

import org.springframework.boot.context.properties.ConfigurationProperties;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
@ConfigurationProperties("app")
public class AppProperties {

    private String name;
    private String version;

}
