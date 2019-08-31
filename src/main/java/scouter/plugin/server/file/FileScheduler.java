package scouter.plugin.server.file;

import lombok.Setter;
import scouter.server.Logger;
import scouter.util.DateUtil;

import java.io.File;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Setter
public class FileScheduler {

    private final String rootDir;
    private final String patternName;
    private final Date startDate;
    private final String name;
    private int duration;
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
                    Calendar calendar= GregorianCalendar.getInstance();
                    calendar.add(Calendar.DAY_OF_MONTH,duration > 0 ? duration *-1 : duration);
                    final long deleteTimeStd = calendar.getTimeInMillis();
                    final String stdFormatTime = DateUtil.format(deleteTimeStd,"yyyy-MM-dd HH:mm:ss");
                    Logger.println("FL-001","file scheduler start. target="+patternName + ", delete time std = " + stdFormatTime);
                    Arrays.stream(new File(rootDir).listFiles())
                          .filter(f -> {
                              final String p = String.join("",
                                      "^",patternName,".+?");
                              return f.getName().matches(p) && f.lastModified() < deleteTimeStd ;
                          })
                          .peek(f->{
                              Logger.println("FL-002","will delete file counter log : "+ f.getAbsolutePath());
                          })
                          .forEach(f ->f.delete());
                }catch (Throwable e){
                    Logger.printStackTrace(e);
                }
            }
        },this.startDate, TimeUnit.DAYS.toMillis(1));
    }
}
