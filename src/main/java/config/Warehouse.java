package config;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class Warehouse {
    @JacksonXmlProperty(localName = "script")
    public String script;
    public String procName;
}
