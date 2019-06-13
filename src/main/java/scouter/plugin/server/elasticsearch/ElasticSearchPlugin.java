package scouter.plugin.server.elasticsearch;

import scouter.lang.CountryCode;
import scouter.lang.TimeTypeEnum;
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
import scouter.util.*;

import java.util.*;

/**
 * @author Heo Yeo Song (yosong.heo@gmail.com) on 2019. 6. 13.
 */
public class ElasticSearchPlugin {

    private final HttpClient httpClient;
    Configure conf = Configure.getInstance();

    private static final String ext_plugin_es_enabled           = "ext_plugin_es_enabled";
    private static final String ext_plugin_es_counter_index     = "ext_plugin_es_counter_index";
    private static final String ext_plugin_es_xlog_index        = "ext_plugin_es_xlog_index";
    private static final String ext_plugin_ex_duration_day      = "ext_plugin_ex_duration_day";

    private static final String ext_plugin_es_https_enabled     = "ext_plugin_es_https_enabled";
    private static final String ext_plugin_es_cluster_address   = "ext_plugin_es_cluster_address";

    private static final String ext_plugin_es_id                = "ext_plugin_es_id";
    private static final String ext_plugin_es_password          = "ext_plugin_es_password";



    final PluginHelper helper       = PluginHelper.getInstance();

    boolean enabled                 = conf.getBoolean(ext_plugin_es_enabled, true);
    private String esCouterIndexName      = conf.getValue(ext_plugin_es_counter_index, "scouter-counter");
    private String esXlogIndexName      = conf.getValue(ext_plugin_es_xlog_index, "scouter-xlog");
    private int esIndexDuration      = conf.getInt(ext_plugin_ex_duration_day, 90);
    boolean esIsHttpSecure          = conf.getBoolean(ext_plugin_es_https_enabled, false);
    String esHttpAddress            = conf.getValue(ext_plugin_es_cluster_address, "127.0.0.1:9200");
    String esUser                   = conf.getValue(ext_plugin_es_id, "");
    String esPassword               = conf.getValue(ext_plugin_es_password, "");

    public ElasticSearchPlugin() {

        this.httpClient = HttpClient.builder()
                .address(esHttpAddress)
                .user(esUser)
                .password(esPassword)
                .isHttps(esIsHttpSecure)
                .build();

        this.httpClient.init();

        Timer jobScheduler = new Timer(true);

        jobScheduler.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                httpClient.flush();
            }
        }, 1, DateTimeHelper.MILLIS_PER_SECOND);

        Timer deleteScheduler = new Timer(true);
        deleteScheduler.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                httpClient.deleteIndex(Arrays.asList(esCouterIndexName,esXlogIndexName),esIndexDuration);
            }
        }, 1, DateTimeHelper.MILLIS_PER_DAY);

        ConfObserver.put("ElasticPluginPlugin", new Runnable() {
            public void run() {
                enabled                 = conf.getBoolean(ext_plugin_es_enabled, true);
                esCouterIndexName       = conf.getValue(ext_plugin_es_counter_index, "scouter-counter");
                esXlogIndexName         = conf.getValue(ext_plugin_es_xlog_index, "scouter-xlog");
                esIndexDuration         = conf.getInt(ext_plugin_ex_duration_day, 90);
                esIsHttpSecure          = conf.getBoolean(ext_plugin_es_https_enabled, false);
                esHttpAddress           = conf.getValue(ext_plugin_es_cluster_address, "127.0.0.1:9200");
                esUser                  = conf.getValue(ext_plugin_es_id, "");
                esPassword              = conf.getValue(ext_plugin_es_password, "");

                httpClient.setAddress(esHttpAddress);
                httpClient.setHttps(esIsHttpSecure);
                httpClient.setUser(esUser);
                httpClient.setPassword(esPassword);
                httpClient.reload();
            }
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
            String objType = AgentManager.getAgent(objHash).objType;
            String objFamily = CounterManager.getInstance().getCounterEngine().getObjectType(objType).getFamily().getName();

            Map<String, Value> dataMap = pack.data.toMap();
            Map<String,Object> _source = new LinkedHashMap<>();

            _source.put("bucket_time",new Date(pack.time));
            _source.put("objHash",Hexa32.toString32(objHash));
            _source.put("objName",objName);
            _source.put("objType",objType);
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

            String _indexName = String.join("-",esCouterIndexName.toLowerCase(), DateUtil.format(System.currentTimeMillis(),"yyyy-MM-dd"));
            final int _id = HashUtil.hash(String.join("",
                                                            _indexName ,
                                                            String.valueOf(objHash) ,
                                                            String.valueOf(System.nanoTime())
                                            ));


            this.httpClient.put(_indexName,String.valueOf(_id),_source);

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

        _source.put("bucket_time",new Date(p.endTime - p.elapsed));
        _source.put("endTime",new Date(p.endTime));
        _source.put("start_time_number",p.endTime - p.elapsed);
        _source.put("end_time_number",p.endTime);
        _source.put("objHash",Hexa32.toString32(p.objHash));
        _source.put("service",this.getString(helper.getServiceString(p.service)));
        _source.put("threadName",this.getString(helper.getHashMsgString(p.threadNameHash)));

        _source.put("txid",Hexa32.toString32(p.txid));
        _source.put("caller",Hexa32.toString32(p.caller));
        _source.put("gxid",Hexa32.toString32(p.gxid));

        _source.put("elapsed",p.elapsed);
        _source.put("error",this.getString(helper.getHashMsgString(p.error)));
        _source.put("cpu",p.cpu);
        _source.put("sqlCount",p.sqlCount);
        _source.put("sqlTime",p.sqlTime);
        _source.put("ipaddr",this.ipByteToString(p.ipaddr));
        _source.put("kbytes",p.kbytes);
        _source.put("userAgent",this.getString(helper.getUserAgentString(p.userAgent)));
        _source.put("referrer",this.getString(helper.getRefererString(p.referer)));
        _source.put("group",this.getString(helper.getUserGroupString(p.group)));
        _source.put("apicallCount",p.apicallCount);
        _source.put("apicallTime",p.apicallTime);
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

        String _indexName = String.join("-",esXlogIndexName, DateUtil.format(System.currentTimeMillis(),"yyyy-MM-dd"));
        final int _id = HashUtil.hash(String.join("",
                _indexName ,
                String.valueOf(p.txid) ,
                String.valueOf(System.nanoTime())
        ));
        this.httpClient.put(_indexName,String.valueOf(_id),_source);

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
