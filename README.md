# scouter-plugin-server-elasticsearch
                                              
![Korean](https://img.shields.io/badge/language-Korean-blue.svg)
- Scouter Server Plugin으로 성능 counter 정보 와 XLOG 정보를 ElasticSearch 로 전송해 주는 plugin 이다.  

### configuration (스카우터 서버 설치 경로 하위의 conf/scouter.conf)
#### 기본 설정
* **ext_plugin_es_enabled** : 본 plugin 사용 여부 (default : true)
* **ext_plugin_es_index** : elasticsearch index 명 (default : scouter-counter)


#### http 방식 연동 여부 설정
* **ext_plugin_es_https_enabled** : https 사용 여부  (default : http 사용)
* **ext_plugin_es_cluster_address** : http target(elasticsearch) address (default : 127.0.0.1:9200)
  - 엘라스틱서치를  쿨러스터로 운영 중면 콤마로 구분 지어 붙인다. ex) 127.0.0.1:9200,127.0.0.1:9201      
* **ext_plugin_es_id** : (default : empty)
* **ext_plugin_es_password** : (default : empty)
    
### dependencies
Refer to [pom.xml](./pom.xml)

### Build environment 
 - Java 1.8.x
 - Maven 3.x 

### Build
 - mvn clean install
    
### Deploy
 - target에 생성되는 scouter-plugin-server-elasticsearch-x.x.x.jar 와 target/lib에 생성되는 전체 library를 scouter sever의 lib 디렉토리에 저장하고 scouter server를 재시작한다
### Support Scouter Version
 - 2.0.x 이상  
### Support ElasticSearch Version
 - 7.0.1 
  
 
