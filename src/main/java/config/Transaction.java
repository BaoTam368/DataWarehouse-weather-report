package config;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import java.util.List;

public class Transaction {
    @JacksonXmlElementWrapper(useWrapping = false)
    @JacksonXmlProperty(localName = "scripts")
    public List<String> scripts;

}