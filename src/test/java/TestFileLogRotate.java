import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scouter.plugin.server.file.FileLogRotate;

import java.io.File;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestFileLogRotate {

    private String saveDir;

    @Before
    public void before(){
        this.saveDir = "/home/kranian/logging";
    }
    @Test
    public void testFileLogging() throws IOException {

        FileLogRotate fileLogRotate = new FileLogRotate("scouter-counter","csv",this.saveDir);
        fileLogRotate.create();
        Assert.assertEquals(true,new File(this.saveDir).exists());
//        fileLogRotate.execute();

        Map<String,Object> data = new LinkedHashMap<>();

        for(int iter=0;iter<10;iter++){
            data.put(String.valueOf("k"+iter),iter);
        }
        fileLogRotate.execute(data);



    }
}