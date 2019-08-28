package scouter.plugin.server.file;

import scouter.server.Logger;

import java.io.*;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;



public class FileLogRotate {

    private final String name;
    private final String dir;
    private final String fileName;
    private final DateTimeFormatter dateformatter;
    private long lastTime;
    PrintWriter dataFile;

    public FileLogRotate(String name,String dir){
        this.name= name;
        this.dir = dir;
        this.fileName = String.join(File.separator,dir,name)+".log";

        this.lastTime = System.currentTimeMillis();
        this.dateformatter = DateTimeFormatter
                .ofPattern("yyyy-MM-dd")
                .withZone(ZoneId.systemDefault());
    }
    protected void create(){
        try {
            dataFile = new PrintWriter(new FileWriter(fileName),true);
        } catch (IOException e) {
            Logger.println("ES003-File not create", e.getMessage());
        }
    }
    protected void rotate() throws FileNotFoundException {
        Calendar calendar= GregorianCalendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH,-1);
        final String rotateDate =dateformatter.format(calendar.getTime().toInstant());

        final String rotateName = String.join("",name,"-",rotateDate,".log");
        final File src = new File(this.fileName);
        final File dest = new File(String.join(File.separator,dir,rotateName));
        if (src.exists()) {
            final boolean isSuccess = src.renameTo(dest);
            if(isSuccess){
                PrintWriter writer = new PrintWriter(src);
                writer.print("");
                writer.close();
            }
        }
    }
    public void execute(List<Object> data){
        final String now  = dateformatter.format(new Date().toInstant());
        final String last = dateformatter.format(new Date(this.lastTime).toInstant());

        if(!Objects.equals(now,last)){
            try {
                this.rotate();
            }catch (IOException e){
                Logger.println("ES003-Counter log rotate error"+e.getMessage());
            }
        }
        dataFile.println( data.stream().map(Object::toString).collect(Collectors.joining(",")) );
        dataFile.flush();
        this.lastTime = System.currentTimeMillis();

    }

}
