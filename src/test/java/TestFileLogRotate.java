import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scouter.plugin.server.file.FileLogRotate;
import scouter.util.DateTimeHelper;
import scouter.util.DateUtil;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class TestFileLogRotate {

    private String saveDir;

    @Before
    public void before(){
        this.saveDir = "/home/kranian/logging";
    }
    @Test
    public void testFileLogging() throws IOException {

        FileLogRotate fileLogRotate = new FileLogRotate("scouter-counter","csv",this.saveDir,this.saveDir);
        fileLogRotate.create();
        Assert.assertEquals(true,new File(this.saveDir).exists());
//        fileLogRotate.execute();

        Map<String,Object> data = new LinkedHashMap<>();

        for(int iter=0;iter<10;iter++){
            data.put(String.valueOf("k"+iter),iter);
        }
        fileLogRotate.execute(data);
    }

    @Test
    public void testTimeUnit(){
        System.out.println(TimeUnit.DAYS.toMillis(3));
    }

    @Test
    public void testCalendar(){
        final Calendar calendar = Calendar.getInstance();
        calendar.set(Calendar.HOUR_OF_DAY,24);
        calendar.set(Calendar.MINUTE,0);
        final Date date = new Date(calendar.getTimeInMillis());
        System.out.println(DateUtil.format(date.getTime(),"yyyy-MM-dd HH:mm:ss"));

    }
    @Test
    public void testPattern(){
        final Pattern compile = Pattern.compile("scouter-counter");
        final String find = "scouter-counter-2019-08-30";

//        Assert.assertEquals("pom.xml", name);
        Assert.assertEquals(true,find.matches("^scouter-counter.+?2019-08-30"));

    }
}