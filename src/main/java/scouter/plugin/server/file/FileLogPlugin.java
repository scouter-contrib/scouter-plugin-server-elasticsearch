package scouter.plugin.server.file;

import scouter.lang.CountryCode;
import scouter.lang.TimeTypeEnum;
import scouter.lang.pack.ObjectPack;
import scouter.lang.pack.PerfCounterPack;
import scouter.lang.pack.XLogPack;
import scouter.lang.plugin.PluginConstants;
import scouter.lang.plugin.annotation.ServerPlugin;
import scouter.lang.value.Value;
import scouter.server.ConfObserver;
import scouter.server.Configure;
import scouter.server.CounterManager;
import scouter.server.Logger;
import scouter.server.core.AgentManager;
import scouter.server.plugin.PluginHelper;
import scouter.util.DateTimeHelper;
import scouter.util.HashUtil;
import scouter.util.Hexa32;
import scouter.util.StringUtil;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author Heo Yeo Song (yosong.heo@gmail.com) on 2019. 6. 13.
 */
public class FileLogPlugin {



    Configure conf = Configure.getInstance();

    private static final String ext_plugin_fl_enabled               = "ext_plugin_fl_enabled";
    private static final String ext_plugin_fl_counter_index         = "ext_plugin_fl_counter_index";
    private static final String ext_plugin_fl_xlog_index            = "ext_plugin_fl_xlog_index";
    private static final String ext_plugin_fl_couter_duration_day   = "ext_plugin_fl_counter_duration_day";
    private static final String ext_plugin_fl_xlog_duration_day     = "ext_plugin_fl_xlog_duration_day";
    private static final String ext_plugin_fl_rotate_dir            = "ext_plugin_fl_rotate_dir";
    private static final String ext_plugin_fl_extension             = "ext_plugin_fl_extension";






    final PluginHelper helper;
    boolean enabled;                                              
    private String couterIndexName;
    private String xlogIndexName;
    private int counterDuration;
    private int xlogDuration;
    String rootDir;
    String extension;
    
    final DateTimeFormatter dateTimeFormatter;



    public FileLogPlugin() {

        this.dateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmss.SSSZ").withZone(ZoneId.systemDefault());
        this.helper             = PluginHelper.getInstance();
        this.enabled            = conf.getBoolean(ext_plugin_fl_enabled, true);
        this.couterIndexName    = conf.getValue(ext_plugin_fl_counter_index, "scouter-counter");
        this.xlogIndexName      = conf.getValue(ext_plugin_fl_xlog_index, "scouter-xlog");
        this.counterDuration    = conf.getInt(ext_plugin_fl_couter_duration_day, 7);
        this.xlogDuration       = conf.getInt(ext_plugin_fl_xlog_duration_day, 3);
        this.rootDir            = conf.getValue(ext_plugin_fl_rotate_dir, "ext_plugin_filelog");
        this.extension          = conf.getValue(ext_plugin_fl_extension, "json");

        ConfObserver.put("FileLogPluginPlugin", ()-> {
            this.enabled            = conf.getBoolean(ext_plugin_fl_enabled, true);
            this.counterDuration    = conf.getInt(ext_plugin_fl_couter_duration_day, 7);
            this.xlogDuration       = conf.getInt(ext_plugin_fl_xlog_duration_day, 3);
            this.extension          = conf.getValue(ext_plugin_fl_extension, "json");
        });
    }


    @ServerPlugin(PluginConstants.PLUGIN_SERVER_COUNTER)
    public void counter(final PerfCounterPack pack) {
        if (!enabled) {
            return;
        }

        if(pack.timetype != TimeTypeEnum.REALTIME) {
            return;
        }

        try {
            String objName = pack.objName;
            int objHash = HashUtil.hash(objName);
            ObjectPack op= AgentManager.getAgent(objHash);
            String objFamily = CounterManager.getInstance().getCounterEngine().getObjectType(op.objType).getFamily().getName();

            Map<String, Value> dataMap = pack.data.toMap();
            Map<String,Object> _source = new LinkedHashMap<>();

            _source.put("bucket_time", this.dateTimeFormatter.format(new Date(pack.time).toInstant()));
            _source.put("objName",op.objName);
            _source.put("objHash",Hexa32.toString32(objHash));
            _source.put("objType",op.objType);
            _source.put("objFamily",objFamily);

            for (Map.Entry<String, Value> field : dataMap.entrySet()) {
                Value valueOrigin = field.getValue();
                if (Objects.isNull(valueOrigin)) {
                    continue;
                }
                Object value = valueOrigin.toJavaObject();
                if(!(value instanceof Number)) {
                    continue;
                }
                String key = field.getKey();
                if(Objects.equals("time",key) || Objects.equals("objHash",key)) {
                    continue;
                }
                _source.put(key,value);
            }



        } catch (Exception e) {
            if (conf._trace) {
                Logger.printStackTrace("ES001", e);
            } else {
                Logger.println("ES002", e.getMessage());
            }
        }
    }

    @ServerPlugin(PluginConstants.PLUGIN_SERVER_XLOG)
    public void xlog(final XLogPack p) {
        if (!enabled) {
            return;
        }

        Map<String,Object> _source = new LinkedHashMap<>();
        ObjectPack op= AgentManager.getAgent(p.objHash);

        _source.put("objName",op.objName);
        _source.put("objHash",Hexa32.toString32(p.objHash));

        _source.put("startTime",this.dateTimeFormatter.format(new Date(p.endTime - p.elapsed).toInstant()));
        _source.put("endTime",this.dateTimeFormatter.format(new Date(p.endTime).toInstant()));

        _source.put("startTimeEpoch",p.endTime - p.elapsed);
        _source.put("endTimeEpoch",p.endTime);


        _source.put("service",this.getString(helper.getServiceString(p.service)));
        _source.put("threadName",this.getString(helper.getHashMsgString(p.threadNameHash)));

        _source.put("txId",Hexa32.toString32(p.txid));
        _source.put("caller",Hexa32.toString32(p.caller));
        _source.put("gxId",Hexa32.toString32(p.gxid));

        _source.put("elapsed",p.elapsed);
        _source.put("error",p.error);
        _source.put("cpu",p.cpu);
        _source.put("sqlCount",p.sqlCount);
        _source.put("sqlTime",p.sqlTime);
        _source.put("ipAddr",this.ipByteToString(p.ipaddr));
        _source.put("memory",p.kbytes);
        _source.put("userAgent",this.getString(helper.getUserAgentString(p.userAgent)));
        _source.put("referrer",this.getString(helper.getRefererString(p.referer)));
        _source.put("group",this.getString(helper.getUserGroupString(p.group)));

        _source.put("apiCallCount",p.apicallCount);
        _source.put("apiCallTime",p.apicallTime);

        _source.put("countryCode", this.getString(p.countryCode));
        _source.put("country", this.getString(CountryCode.getCountryName(this.getString(p.countryCode))));
        _source.put("city",this.getString(helper.getCityString(p.city)));
        _source.put("login",this.getString(helper.getLoginString(p.login)));
        _source.put("desc",this.getString(helper.getDescString(p.desc)));

        _source.put("text1",this.getString(p.text1));
        _source.put("text2",this.getString(p.text2));
        _source.put("text3",this.getString(p.text3));
        _source.put("text4",this.getString(p.text4));
        _source.put("text5",this.getString(p.text5));
        _source.put("queuingHostHash",this.getString(helper.getHashMsgString(p.queuingHostHash)));
        _source.put("queuingTime",p.queuingTime);
        _source.put("queuing2ndHostHash",this.getString(helper.getHashMsgString(p.queuingHostHash)));
        _source.put("queuing2ndTime",p.queuing2ndTime);



    }
    private String getString(String value){
        return StringUtil.nullToEmpty(value);
    }

    private String ipByteToString(byte[] ip) {
        if (ip == null)
            return "0.0.0.0";
        try {
            StringBuilder sb = new StringBuilder();
            sb.append(ip[0] & 0xff);
            sb.append(".");
            sb.append(ip[1] & 0xff);
            sb.append(".");
            sb.append(ip[2] & 0xff);
            sb.append(".");
            sb.append(ip[3] & 0xff);
            return sb.toString();
        } catch (Throwable e) {
            return "0.0.0.0";
        }
    }
}
