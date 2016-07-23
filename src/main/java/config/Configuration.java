package config;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * Reads configuration file.
 * 
 * @author aaronzhang
 */
public class Configuration {
    
    /**
     * File to read.
     */
    private final File file;
    
    /**
     * Settings and values.
     */
    private final Map<String, String> settings = new HashMap<>();
    
    /**
     * Constructs a new reader for the specified filename.
     * 
     * @param file filename
     */
    public Configuration(String file) {
        this.file = new File(file);
    }
    
    /**
     * Constructs a new reader for the specified file.
     * 
     * @param file file
     */
    public Configuration(File file) {
        this.file = file;
    }
    
    /**
     * Reads configuration file.
     * 
     * @throws IOException if error reading file
     * @throws ParseException if error parsing file
     */
    public void read() throws IOException, ParseException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                new FileInputStream(file), "UTF-8"))) {
            int lineNum = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isEmpty() && !line.startsWith("#")) {
                    if (!line.contains("=")) {
                        throw new ParseException("error parsing file", lineNum);
                    }
                    String[] pieces = line.split("=", 2);
                    settings.put(
                        pieces[0], pieces[1].isEmpty() ? null : pieces[1]);
                }
                lineNum++;
            }
        }
    }
    
    /**
     * @return the file
     */
    public File getFile() {
        return file;
    }
    
    /**
     * Gets value for given setting, or {@code null} if no value specified in
     * configuration file.
     * 
     * @param key
     * @return 
     */
    public String get(String key) {
        return settings.get(key);
    }
    
    /**
     * Gets value for given setting, or {@code defaultValue} if no value
     * specified in configuration file.
     * 
     * @param key
     * @param defaultValue
     * @return 
     */
    public String getOrDefault(String key, String defaultValue) {
        return settings.getOrDefault(key, defaultValue);
    }
    
    @Override
    public String toString() {
        return String.format("[Configuration: file=%s]", file);
    }
}