package config;

import com.fasterxml.jackson.dataformat.xml.annotation.*;

@JacksonXmlRootElement(localName = "config")
public class Config {

    public Database database;
    public Source source;
    public Extract extract;
    public Transaction transaction;
}