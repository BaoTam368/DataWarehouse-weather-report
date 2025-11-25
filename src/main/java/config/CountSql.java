package config;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class CountSql {
    @JacksonXmlProperty(localName = "path")
    public String path;
}
