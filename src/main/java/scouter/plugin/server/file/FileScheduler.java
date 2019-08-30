package scouter.plugin.server.file;

import scouter.server.Logger;

import java.io.File;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public class FileScheduler {

    private final String rootDir;
    private final String patternName;
    private final Date startDate;
    private final String name;
    private final int duration;
    private final DateTimeFormatter dateTimeFormater;

    public FileScheduler(String rootDir, String patternName, String name, Date startDate, int duration, DateTimeFormatter dateTimeFormatter){
        this.rootDir = rootDir;
        this.patternName = patternName;
        this.startDate = startDate;
        this.name = name;
        this.duration = duration;
        this.dateTimeFormater = dateTimeFormatter;
    }


    public void start(){
        final Timer timerCounter = new Timer(this.name,true);
        timerCounter.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    Logger.println("file scheduler start. target="+patternName);
                    Arrays.stream(new File(rootDir).listFiles())
                          .filter(f -> {
                              final String p = String.join("",
                                      "^",patternName,".+?"
                                      ,dateTimeFormater.format(new Date().toInstant()));
                              return f.getName().matches(p);
                          })
                          .forEach(f ->f.delete());
                }catch (Throwable e){
                    Logger.printStackTrace(e);
                }
            }
        },this.startDate, TimeUnit.DAYS.toMillis(this.duration));
    }
}
