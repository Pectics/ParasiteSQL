package me.pectics.parasitesql;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.pattern.DynamicConverter;
import org.jetbrains.annotations.NotNull;

/**
 * 自定义 Level 缩写转换器：
 * INFO -> INFO
 * WARN -> WARN
 * ERROR -> ERRR
 * DEBUG -> DEBG
 * 其他 -> 原始 LEVEL 名（大写）
 */
public class AbbrLevelConverter extends DynamicConverter<ILoggingEvent> {

    @Override
    public String convert(@NotNull ILoggingEvent event) {
        Level level = event.getLevel();
        return switch (level.toInt()) {
            case Level.INFO_INT -> "INFO";
            case Level.WARN_INT -> "WARN";
            case Level.ERROR_INT -> "ERRR";
            case Level.DEBUG_INT -> "DEBG";
            default -> level.levelStr.toUpperCase();
        };
    }

}
