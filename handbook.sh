#!/usr/bin/env bash
export CLASSPATH="$(find target/ -type f -name '*.jar'| grep '\-package' | tr '\n' ':')"
if hash docker 2>/dev/null; then
    # for docker lovers
    docker build . -t meirkhan/kafka-connect-project-1.0-SNAPSHOT
    docker run --net=host --rm -t \
           -v $(pwd)/offsets:/kafka-connect-project/offsets \
           meirkhan/kafka-connect-project-1.0-SNAPSHOT
elif hash connect-standalone 2>/dev/null; then
    # for mac users who used homebrew
    connect-standalone config/worker.properties config/MySourceConnectorExample.properties
elif [[ -z $KAFKA_HOME ]]; then
    # for people who installed kafka vanilla
    $KAFKA_HOME/bin/connect-standalone.sh $KAFKA_HOME/etc/schema-registry/connect-avro-standalone.properties config/MySourceConnectorExample.properties
elif [[ -z $CONFLUENT_HOME ]]; then
    # for people who installed kafka confluent flavour
    $CONFLUENT_HOME/bin/connect-standalone $CONFLUENT_HOME/etc/schema-registry/connect-avro-standalone.properties config/MySourceConnectorExample.properties
else
    printf "Couldn't find a suitable way to run kafka connect for you.\n \
Please install Docker, or download the kafka binaries and set the variable KAFKA_HOME."
fi;


Changed dir for kafka connect pluins: For debezium and MonoSink connectors
docker run -it --rm -p 2181:2181 -p 3030:3030 -p 8081:8081 -p 8082:8082 -p 8083:8083 -p 9092:9092 -e ADV_HOST=127.0.0.1 -e RUNTESTS=0 -v ~/projects/kafkaconnectgithub/target/kafka-connect-project-1.0-SNAPSHOT-package/share/java/kafka-connect-project:/connectors landoop/fast-data-dev
docker run -it --rm -p 2182:2182 -p 3030:3030 -p 8084:8084 -p 8085:8085 -p 8086:8086 -p 9094:9094 -e ADV_HOST=127.0.0.1 -e RUNTESTS=0 -v ~/projects/kafkaconnectgithub/target/kafka-connect-project-1.0-SNAPSHOT-package/share/java/kafka-connect-project:/connectors landoop/fast-data-dev

docker issues:
- gave super rights to docker
- Create a file named /etc/systemd/system/docker.service.d/10_docker_proxy.conf with below content
[Service]
Environment=HTTP_PROXY=http://10.0.2.2:3128
Environment=HTTPS_PROXY=http://10.0.2.2:3128



Run Helloworld KafkaSourceConnector
1. Removed parts with HTTP client in GithubSourceConnector
2. mvn clean package
3. run confluent/start
4. sudo bash run.sh with only CONFLUENT_HOME
5. if error "address is already in use", kill process

6. curl -s "http://localhost:8083/connectors" ->>to see connectors list
7. curl -s "http://localhost:8083/connectors/jdbc_source_mysql_10/status"|jq '.' ->> see status of connector
8. curl -s "http://localhost:8083/connectors/jdbc_source_mysql_10/config"|jq '.' ->> config of the connector



-------Mongo-----------
--Mongo Bulk Insert
exported mongo_path at bashrc


-------------------------Mongo queries---------------------------------
show dbs
use test
db.products.insert( { _id: 10, item: "pen", qty: 25 } )
db.products.insert( { _id: 10, item: "pen", qty: 30 } )
db.products.insert( { _id: 10, item: "pen", qty: 40 } )

db.transactions.insert(
   [{  "dateTime":"2016-11-02T06:49:48.256+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.USER_PHONES",  "data":{"USER_PHONES_ID":"341370", "INVALID_PIN_COUNT":"0"},  "before":{"USER_PHONES_ID":"341370", "INVALID_PIN_COUNT":"0"} },
{  "dateTime":"2016-11-02T06:49:48.273+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.USER_PHONES",  "data":{"USER_PHONES_ID":"341370", "LAST_TRANSACTION_ON":"2016-11-02T06:49:47.000+01:00", "LAST_TRANSFER_ID":"CI161102.0649.B23172", "INVALID_PIN_COUNT":"0", "LAST_TRANSFER_TYPE":"CASHIN"},  "before":{"USER_PHONES_ID":"341370", "LAST_TRANSACTION_ON":"2016-10-27T10:22:29.000+02:00", "LAST_TRANSFER_ID":"CI161027.1022.B23142", "INVALID_PIN_COUNT":"0", "LAST_TRANSFER_TYPE":"CASHIN"} },
{  "dateTime":"2016-11-02T06:49:48.273+01:00",  "OperationName":"UPDATE",  "TableName":"SYN_MMONEY.MTX_WALLET_BALANCES",  "data":{"LAST_TRANSFER_BALANCE_ACTION":"DR", "LAST_TRANSFER_ON":"2016-11-02T06:49:47.000+01:00", "BALANCE_DEBIT":"210619800", "LAST_TRANSFER_ID":"CI161102.0649.B23172", "WALLET_NUMBER":"1106141534000005", "LAST_TRANSFER_TYPE":"CASHIN", "FROZEN_AMOUNT":"0", "PREVIOUS_BALANCE":"9818199600", "WALLET_SEQUENCE_NUMBER":"0", "BALANCE":"9818189500"},  "before":{"LAST_TRANSFER_BALANCE_ACTION":"DR", "LAST_TRANSFER_ON":"2016-10-27T10:22:29.000+02:00", "BALANCE_DEBIT":"210609700", "LAST_TRANSFER_ID":"CI161027.1022.B23142", "WALLET_NUMBER":"1106141534000005", "LAST_TRANSFER_TYPE":"CASHIN", "FROZEN_AMOUNT":"0", "PREVIOUS_BALANCE":"9818209700", "WALLET_SEQUENCE_NUMBER":"0", "BALANCE":"9818199600"} },
{  "dateTime":"2016-11-02T06:49:48.286+01:00",  "OperationName":"UPDATE",  "TableName":"SYN_MMONEY.MTX_WALLET_BALANCES",  "data":{"LAST_TRANSFER_BALANCE_ACTION":"CR", "LAST_TRANSFER_ON":"2016-11-02T06:49:47.000+01:00", "BALANCE_CREDIT":"16373800", "LAST_TRANSFER_ID":"CI161102.0649.B23172", "WALLET_NUMBER":"1101191157000004", "LAST_TRANSFER_TYPE":"CASHIN", "FROZEN_AMOUNT":"0", "PREVIOUS_BALANCE":"10010938700", "WALLET_SEQUENCE_NUMBER":"0", "BALANCE":"10010948800"},  "before":{"LAST_TRANSFER_BALANCE_ACTION":"CR", "LAST_TRANSFER_ON":"2016-10-26T12:46:16.000+02:00", "BALANCE_CREDIT":"16363700", "LAST_TRANSFER_ID":"CI161026.1246.B21806", "WALLET_NUMBER":"1101191157000004", "LAST_TRANSFER_TYPE":"CASHIN", "FROZEN_AMOUNT":"0", "PREVIOUS_BALANCE":"10010928600", "WALLET_SEQUENCE_NUMBER":"0", "BALANCE":"10010938700"} },
{  "dateTime":"2016-11-02T06:49:48.286+01:00",  "OperationName":"INSERT",  "TableName":"SYN_MMONEY.MTX_TRANSACTION_HEADER",  "data":{"TRANSFER_VALUE":"10100", "TRANSFER_RULE_ID":"327", "REMARKS":"null", "TXN_INSTANCE_ID":"null", "REQUEST_GATEWAY_TYPE":"USSD", "PIN_SENT_TO":"77990438", "COUNTRY":"null", "TOTAL_TAXES":"0", "REQUESTED_VALUE":"10100", "BATCH_DATE":"null", "REQUEST_SOURCE":"BROWSER", "SERVICE_CHARGE_SET_ID":"567903", "BATCH_NUMBER":"null", "ATTR_2_VALUE":"null", "LANGUAGE":"null", "CREATED_BY":"PT110614.1534.000005", "CREDIT_BACK_STATUS":"null", "TRANSFER_ON":"2016-11-02T06:49:47.000+01:00", "SERVICE_CHARGE_RULE_ID":"567903", "ATTEMPT_STATUS":"null", "ATTR_3_NAME":"null", "ATTR_1_VALUE":"null", "CREATED_ON":"2016-11-02T06:49:47.000+01:00", "REQUEST_GATEWAY_CODE":"CELLUSSD", "RECONCILIATION_DONE":"null", "MODIFIED_BY":"PT110614.1534.000005", "TRANSFER_SUBTYPE":"CASHIN", "APPLICATION_ID":"2", "TRANSFER_STATUS":"TS", "BANK_ID":"null", "EXT_TXN_NUMBER":"null", "ATTR_1_NAME":"CELL_ID", "TOTAL_OPT_SERVICE_CHARGE":"0", "ERROR_CODE":"null", "SERIAL_NUMBER":"null", "SUBSERVICE":"null", "PAYMENT_METHOD_TYPE":"WALLET", "FOC_BONUS_BATCH_NUMBER":"null", "TRANSFER_CATEGORY":"null", "TOTAL_SERVICE_CHARGE":"0", "REQUEST_THROUGH_QUEUE":"null", "MODIFIED_ON":"2016-11-02T06:49:47.000+01:00", "CONTROLLED_TRANSFER":"null", "TOTAL_DEBITED_VALUE":"10100", "SERVICE_CHARGE_DETAILSID":"null", "RECONCILIATION_BY":"null", "ATTR_2_NAME":"FTXN_ID", "TOTAL_CREDITED_VALUE":"10100", "REFERENCE_NUMBER":"null", "FOC_BONUS_BATCH_DATE":"null", "TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "RECONCILIATION_DATE":"null", "CLOSE_DATE":"null", "GRPH_DOMAIN_CODE":"null", "TRANSFER_ID":"CI161102.0649.B23172", "TRANSFER_TYPE":"TR_TRF", "EXT_TXN_DATE":"null", "VERSION":"0", "SERVICE_CHARGE_SET_VERSION":"14", "ATTR_3_VALUE":"null", "SERVICE_TYPE":"CASHIN"},  "before":{"i" : 1} },
{  "dateTime":"2016-11-02T06:49:48.370+01:00",  "OperationName":"INSERT",  "TableName":"SYN_MMONEY.MTX_TRANSACTION_ITEMS",  "data":{"SECOND_PTY_PAYMENT_METHOD_DESC":"12", "ACCOUNT_ID":"77990438", "UNIT_PRICE":"1", "TRANSFER_VALUE":"10100", "ENTRY_TYPE":"DR", "REQUESTED_VALUE":"10100", "PREF_LANGUAGE":"2", "ATTR_2_VALUE":"null", "CATEGORY_CODE":"RETAILER", "THIRD_PARTY_FAIL_REASON":"null", "SECOND_PSEUDO_USER_ID":"null", "TRANSFER_ON":"2016-11-02T06:49:47.000+01:00", "FIRST_PTY_PAYMENT_METHOD_DESC":"12", "ATTR_3_NAME":"null", "UNREG_USER_ID":"null", "USER_TYPE":"PAYER", "APPROVED_VALUE":"10100", "ATTR_1_VALUE":"null", "ACCESS_TYPE":"PHONE", "PARTY_ID":"PT110614.1534.000005", "PAYMENT_TYPE_ID":"12", "SECOND_PARTY":"PT110119.1157.000004", "ACCOUNT_TYPE":"WALLET", "UNIQUE_SEQ_NUMBER":"B124091165", "SECOND_UNREG_USER_ID":"null", "TRANSFER_SUBTYPE":"CASHIN", "POST_CASH":"null", "BANK_ID":"null", "TRANSFER_STATUS":"TS", "WALLET_NUMBER":"1106141534000005", "ATTR_1_NAME":"null", "PROVIDER_ID":"101", "PAYMENT_METHOD_TYPE":"WALLET", "TXN_SEQUENCE_NUMBER":"1", "SECOND_PARTY_ACCOUNT_ID":"79047579", "TRANSFER_PROFILE_DETAILS_ID":"null", "COMMISSION_SLAB_CODE":"null", "PSEUDO_USER_ID":"null", "POST_BALANCE":"9818189500", "ATTR_2_NAME":"null", "REFERENCE_NUMBER":"null", "PREVIOUS_CASH":"null", "TRANSACTION_TYPE":"MP", "TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "PREVIOUS_BALANCE":"9818199600", "SECOND_PARTY_ACCOUNT_TYPE":"WALLET", "PARTY_ACCESS_ID":"77990438", "GRPH_DOMAIN_CODE":"ML", "TRANSFER_ID":"CI161102.0649.B23172", "SECOND_PARTY_PROVIDER_ID":"101", "TXN_MODE":"null", "ATTR_3_VALUE":"null", "SECOND_PARTY_CATEGORY_CODE":"SUBS", "SERVICE_TYPE":"CASHIN"}},
{  "dateTime":"2016-11-02T06:49:48.382+01:00",  "OperationName":"INSERT",  "TableName":"SYN_MMONEY.MTX_TRANSACTION_ITEMS",  "data":{"SECOND_PTY_PAYMENT_METHOD_DESC":"12", "ACCOUNT_ID":"79047579", "UNIT_PRICE":"1", "TRANSFER_VALUE":"10100", "ENTRY_TYPE":"CR", "REQUESTED_VALUE":"10100", "PREF_LANGUAGE":"2", "ATTR_2_VALUE":"null", "CATEGORY_CODE":"SUBS", "THIRD_PARTY_FAIL_REASON":"null", "SECOND_PSEUDO_USER_ID":"null", "TRANSFER_ON":"2016-11-02T06:49:47.000+01:00", "FIRST_PTY_PAYMENT_METHOD_DESC":"12", "ATTR_3_NAME":"null", "UNREG_USER_ID":"null", "USER_TYPE":"PAYEE", "APPROVED_VALUE":"10100", "ATTR_1_VALUE":"null", "ACCESS_TYPE":"PHONE", "PARTY_ID":"PT110119.1157.000004", "PAYMENT_TYPE_ID":"12", "SECOND_PARTY":"PT110614.1534.000005", "ACCOUNT_TYPE":"WALLET", "UNIQUE_SEQ_NUMBER":"B124091166", "SECOND_UNREG_USER_ID":"null", "TRANSFER_SUBTYPE":"CASHIN", "POST_CASH":"null", "BANK_ID":"null", "TRANSFER_STATUS":"TS", "WALLET_NUMBER":"1101191157000004", "ATTR_1_NAME":"null", "PROVIDER_ID":"101", "PAYMENT_METHOD_TYPE":"WALLET", "TXN_SEQUENCE_NUMBER":"2", "SECOND_PARTY_ACCOUNT_ID":"77990438", "TRANSFER_PROFILE_DETAILS_ID":"null", "COMMISSION_SLAB_CODE":"null", "PSEUDO_USER_ID":"null", "POST_BALANCE":"10010948800", "ATTR_2_NAME":"null", "REFERENCE_NUMBER":"null", "PREVIOUS_CASH":"null", "TRANSACTION_TYPE":"MR", "TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "PREVIOUS_BALANCE":"10010938700", "SECOND_PARTY_ACCOUNT_TYPE":"WALLET", "PARTY_ACCESS_ID":"79047579", "GRPH_DOMAIN_CODE":"ML", "TRANSFER_ID":"CI161102.0649.B23172", "SECOND_PARTY_PROVIDER_ID":"101", "TXN_MODE":"null", "ATTR_3_VALUE":"null", "SECOND_PARTY_CATEGORY_CODE":"RETAILER", "SERVICE_TYPE":"CASHIN"},  "before":"null" },
{  "dateTime":"2016-11-02T06:49:48.382+01:00",  "OperationName":"INSERT",  "TableName":"MMONEY.MTX_AUDIT_TRAIL",  "data":{"CUST_ID":"null", "TRANSACTION_ON":"2016-11-02T06:49:47.000+01:00", "PAYEE_PROVIDER_ID":"101", "ACCOUNT2":"1101191157000004", "ACCOUNT1":"1106141534000005", "PAYER_PAYMENT_TYPE_ID":"12", "COMMENTS":"null", "RECON_ID":"null", "ATTR_1_NAME":"DR", "IDEA_PAYMENT_ID":"null", "TRANSACTION_STATUS":"TS", "ERROR_CODE":"null", "PAYEE_PAYMENT_INSTRUMENT":"WALLET", "LOGGED_OUT":"null", "PAYER_PAYMENT_INSTRUMENT":"WALLET", "TRANSACTION_SUBTYPE":"CASHIN", "SN":"AU161102.0649.B79631", "ATTR_2_VALUE":"10100", "TRANSACTION_DATE":"2016-11-02T06:49:47.000+01:00", "PAYEE_PAYMENT_TYPE_ID":"12", "ATTR_2_NAME":"CR", "PAYER_PROVIDER_ID":"101", "TRANSACTION_AMOUNT":"10100", "ATTR_3_NAME":"COMM_IND03", "PARTY_ACCESS_ID":"77990438", "PAYEE_BANK_ID":"null", "ATTR_1_VALUE":"10100", "TXN_MODE":"null", "ATTR_3_VALUE":"0", "LOGIN_ATTEMPT":"null", "SERVICE_TYPE":"CASHIN", "TRANSACTION_ID":"CI161102.0649.B23172", "PARTY_ID":"PT110614.1534.000005", "LOGGED_IN":"null", "PAYER_BANK_ID":"null"},  "before":"null" },
{  "dateTime":"2016-11-02T06:49:48.397+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1104172358.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC110723.0926.C02764", "PAYER_COUNT":"1", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"1"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1104172358.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"20671800", "ID":"TC110723.0926.C02764", "PAYER_COUNT":"2057", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"1"} },
{  "dateTime":"2016-11-02T06:49:48.408+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1104172358.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC110723.0926.C02765", "PAYER_COUNT":"1", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"2"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1104172358.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"5836800", "ID":"TC110723.0926.C02765", "PAYER_COUNT":"581", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"2"} },
{  "dateTime":"2016-11-02T06:49:48.408+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1104172358.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC110723.0926.C02766", "PAYER_COUNT":"1", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"3"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1104172358.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"341200", "ID":"TC110723.0926.C02766", "PAYER_COUNT":"34", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"3"} },
{  "dateTime":"2016-11-02T06:49:48.408+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1104172358.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC160707.1410.B00241", "PAYER_COUNT":"1", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"1"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1104172358.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"20671800", "ID":"TC160707.1410.B00241", "PAYER_COUNT":"2057", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"1"} },
{  "dateTime":"2016-11-02T06:49:48.408+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1104172358.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC160707.1410.B00242", "PAYER_COUNT":"1", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"2"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1104172358.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"5836800", "ID":"TC160707.1410.B00242", "PAYER_COUNT":"581", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"2"} },
{  "dateTime":"2016-11-02T06:49:48.408+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1104172358.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC160707.1410.B00243", "PAYER_COUNT":"1", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"3"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1104172358.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"341200", "ID":"TC160707.1410.B00243", "PAYER_COUNT":"34", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"3"} },
{  "dateTime":"2016-11-02T06:49:48.408+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP0910300516.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC151015.1107.A24748", "PAYER_COUNT":"1", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"1"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP0910300516.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"20681800", "ID":"TC151015.1107.A24748", "PAYER_COUNT":"2058", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"1"} },
{  "dateTime":"2016-11-02T06:49:48.408+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP0910300516.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC151015.1107.A24749", "PAYER_COUNT":"1", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"2"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP0910300516.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"5836800", "ID":"TC151015.1107.A24749", "PAYER_COUNT":"581", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"2"} },
{  "dateTime":"2016-11-02T06:49:48.409+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP0910300516.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC151015.1107.A24750", "PAYER_COUNT":"1", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"3"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP0910300516.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"341200", "ID":"TC151015.1107.A24750", "PAYER_COUNT":"34", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"3"} },
{  "dateTime":"2016-11-02T06:49:48.409+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP0910300516.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC160707.1410.B00244", "PAYER_COUNT":"1", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"1"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP0910300516.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"20702000", "ID":"TC160707.1410.B00244", "PAYER_COUNT":"2060", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"1"} },
{  "dateTime":"2016-11-02T06:49:48.409+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP0910300516.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC160707.1410.B00245", "PAYER_COUNT":"1", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"2"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP0910300516.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"5836800", "ID":"TC160707.1410.B00245", "PAYER_COUNT":"581", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"2"} },
{  "dateTime":"2016-11-02T06:49:48.409+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP0910300516.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC160707.1410.B00246", "PAYER_COUNT":"1", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"3"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP0910300516.000001", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "OTHER_INFO":"1106141534000005", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"341200", "ID":"TC160707.1410.B00246", "PAYER_COUNT":"34", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"3"} },
{  "dateTime":"2016-11-02T06:49:48.409+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1403230853.065", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC151015.1107.A24754", "PAYER_COUNT":"1", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"1"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1403230853.065", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"20722100", "ID":"TC151015.1107.A24754", "PAYER_COUNT":"2062", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"1"} },
{  "dateTime":"2016-11-02T06:49:48.409+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1403230853.065", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC151015.1107.A24755", "PAYER_COUNT":"1", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"2"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1403230853.065", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"5846800", "ID":"TC151015.1107.A24755", "PAYER_COUNT":"582", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"2"} },
{  "dateTime":"2016-11-02T06:49:48.409+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1403230853.065", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"10100", "ID":"TC151015.1107.A24756", "PAYER_COUNT":"1", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"3"},  "before":{"PAYEE_COUNT":"0", "PROFILE_ID":"TCP1403230853.065", "PAYEE_AMT":"0", "BEARER_ID":"ALL", "USER_ID":"PT110614.1534.000005", "PAYER_AMT":"341200", "ID":"TC151015.1107.A24756", "PAYER_COUNT":"34", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-27T10:22:29.000+02:00", "GROUP_ID":"3"} },
{  "dateTime":"2016-11-02T06:49:48.409+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP0910281055.000001", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC110119.1547.C02867", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"2"},  "before":{"PAYEE_COUNT":"41", "PROFILE_ID":"TCP0910281055.000001", "PAYEE_AMT":"412200", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC110119.1547.C02867", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"2"} },
{  "dateTime":"2016-11-02T06:49:48.409+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP0910281055.000001", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC110119.1547.C02868", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"3"},  "before":{"PAYEE_COUNT":"12", "PROFILE_ID":"TCP0910281055.000001", "PAYEE_AMT":"120700", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC110119.1547.C02868", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"3"} },
{  "dateTime":"2016-11-02T06:49:48.409+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP0910281055.000001", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC110119.1547.C02866", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"1"},  "before":{"PAYEE_COUNT":"131", "PROFILE_ID":"TCP0910281055.000001", "PAYEE_AMT":"1316400", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC110119.1547.C02866", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"1"} },
{  "dateTime":"2016-11-02T06:49:48.409+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP0910281055.000001", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29253", "PAYER_COUNT":"0", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"1"},  "before":{"PAYEE_COUNT":"131", "PROFILE_ID":"TCP0910281055.000001", "PAYEE_AMT":"1316400", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29253", "PAYER_COUNT":"0", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"1"} },
{  "dateTime":"2016-11-02T06:49:48.411+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP0910281055.000001", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29254", "PAYER_COUNT":"0", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"2"},  "before":{"PAYEE_COUNT":"41", "PROFILE_ID":"TCP0910281055.000001", "PAYEE_AMT":"412200", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29254", "PAYER_COUNT":"0", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"2"} },
{  "dateTime":"2016-11-02T06:49:48.411+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP0910281055.000001", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29255", "PAYER_COUNT":"0", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"3"},  "before":{"PAYEE_COUNT":"12", "PROFILE_ID":"TCP0910281055.000001", "PAYEE_AMT":"120700", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29255", "PAYER_COUNT":"0", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"3"} },
{  "dateTime":"2016-11-02T06:49:48.488+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP0910281042.000001", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC160707.1539.A05125", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"1"},  "before":{"PAYEE_COUNT":"131", "PROFILE_ID":"TCP0910281042.000001", "PAYEE_AMT":"1316400", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC160707.1539.A05125", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"1"} },
{  "dateTime":"2016-11-02T06:49:48.490+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP0910281042.000001", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC160707.1539.A05126", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"2"},  "before":{"PAYEE_COUNT":"41", "PROFILE_ID":"TCP0910281042.000001", "PAYEE_AMT":"412200", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC160707.1539.A05126", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"2"} },
{  "dateTime":"2016-11-02T06:49:48.492+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP0910281042.000001", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC160707.1539.A05127", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"3"},  "before":{"PAYEE_COUNT":"12", "PROFILE_ID":"TCP0910281042.000001", "PAYEE_AMT":"120700", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC160707.1539.A05127", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"3"} },
{  "dateTime":"2016-11-02T06:49:48.492+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP0910281042.000001", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29262", "PAYER_COUNT":"0", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"1"},  "before":{"PAYEE_COUNT":"131", "PROFILE_ID":"TCP0910281042.000001", "PAYEE_AMT":"1316400", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29262", "PAYER_COUNT":"0", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"1"} },
{  "dateTime":"2016-11-02T06:49:48.492+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP0910281042.000001", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29263", "PAYER_COUNT":"0", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"2"},  "before":{"PAYEE_COUNT":"41", "PROFILE_ID":"TCP0910281042.000001", "PAYEE_AMT":"412200", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29263", "PAYER_COUNT":"0", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"2"} },
{  "dateTime":"2016-11-02T06:49:48.492+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP0910281042.000001", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29264", "PAYER_COUNT":"0", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"3"},  "before":{"PAYEE_COUNT":"12", "PROFILE_ID":"TCP0910281042.000001", "PAYEE_AMT":"120700", "BEARER_ID":"ALL", "OTHER_INFO":"1101191157000004", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29264", "PAYER_COUNT":"0", "SERVICE_TYPE":"CASHIN", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"3"} },
{  "dateTime":"2016-11-02T06:49:48.492+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP1403230852.491", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29256", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"1"},  "before":{"PAYEE_COUNT":"131", "PROFILE_ID":"TCP1403230852.491", "PAYEE_AMT":"1316400", "BEARER_ID":"ALL", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29256", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"1"} },
{  "dateTime":"2016-11-02T06:49:48.492+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP1403230852.491", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29257", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"2"},  "before":{"PAYEE_COUNT":"41", "PROFILE_ID":"TCP1403230852.491", "PAYEE_AMT":"412200", "BEARER_ID":"ALL", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29257", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"2"} },
{  "dateTime":"2016-11-02T06:49:48.492+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.THRESHOLD_COUNT",  "data":{"PAYEE_COUNT":"1", "PROFILE_ID":"TCP1403230852.491", "PAYEE_AMT":"10100", "BEARER_ID":"ALL", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29258", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-11-02T06:49:47.000+01:00", "GROUP_ID":"3"},  "before":{"PAYEE_COUNT":"12", "PROFILE_ID":"TCP1403230852.491", "PAYEE_AMT":"120700", "BEARER_ID":"ALL", "USER_ID":"PT110119.1157.000004", "PAYER_AMT":"0", "ID":"TC140710.1033.B29258", "PAYER_COUNT":"0", "SERVICE_TYPE":"ALL", "LAST_TRANSFER_DATE":"2016-10-26T12:46:16.000+02:00", "GROUP_ID":"3"} },
{  "dateTime":"2016-11-02T06:49:48.493+01:00",  "OperationName":"UPDATE",  "TableName":"MMONEY.MTX_MAX_TXN_SEQ",  "data":{"NODE_NAME":"B ", "TIME":"2016-11-02T06:49:47.000+01:00", "MAX_SEQ":"B124091166"},  "before":{"NODE_NAME":"B ", "TIME":"2016-10-27T10:22:23.000+02:00", "MAX_SEQ":"B124090342"} },
{  "dateTime":"2016-11-02T06:49:48.503+01:00",  "OperationName":"INSERT",  "TableName":"MMONEY.SENTSMS",  "data":{"MESSAGE":"Vous avez envoye 101.00 FCFA au 79047579 et vous avez recu une commission de 0.00 FCFA. Votre nouveau solde est de 98181895.00 FCFA", "STATUS":"Y", "MSISDN":"77990438", "RETRY_COUNT":"0", "REFERENCE":"CI161102.0649.B23172", "DELIVEREDON":"2016-11-02T06:49:48.000+01:00", "CREATED_ON":"2016-11-02T06:49:48.000+01:00", "IS_DELIVERED":"N", "ID":"SM161102.0649.B74124"},  "before":"null" },
{  "dateTime":"2016-11-02T06:49:48.509+01:00",  "OperationName":"INSERT",  "TableName":"MMONEY.SENTSMS",  "data":{"MESSAGE":"Depot de 101.00 FCFA effectue par 77990438. Votre nouveau solde est de 100109488.00 FCFA. Solde maximum autorise : 1.500.000 FCFA", "STATUS":"Y", "MSISDN":"79047579", "RETRY_COUNT":"0", "REFERENCE":"CI161102.0649.B23172", "DELIVEREDON":"2016-11-02T06:49:48.000+01:00", "CREATED_ON":"2016-11-02T06:49:48.000+01:00", "IS_DELIVERED":"N", "ID":"SM161102.0649.B74125"},  "before":"null" }]
)



Links:

1. Why KC exists
http://why-not-learn-something.blogspot.com/2018/02/kafka-connect-why-exists-and-how-works.html

2. Why not to use KC
https://hackernoon.com/why-we-replaced-our-kafka-connector-with-a-kafka-consumer-972e56bebb23

3. Kafka ecosystem overview
https://medium.com/@stephane.maarek/the-kafka-api-battle-producer-vs-consumer-vs-kafka-connect-vs-kafka-streams-vs-ksql-ef584274c1e

4. Kafka Connect JDBC Source
https://www.confluent.io/blog/kafka-connect-deep-dive-jdbc-source-connector

5. Kafka offsets and partitions
https://medium.com/@yusufs/getting-started-with-kafka-in-golang-14ccab5fa26
https://stackoverflow.com/questions/53818612/kafka-will-different-partitions-have-the-same-offset-number
https://grokbase.com/t/kafka/users/143748y6tq/are-offsets-unique-immutable-identifiers-for-a-message-in-a-topic

6. GetMetadata() to return the partition list for a topic (or all topics)
https://docs.confluent.io/current/clients/confluent-kafka-go/index.html#Consumer.GetMetadata

7. DIY Kafka Connector
https://enfuse.io/a-diy-guide-to-kafka-connectors/

8. Custom GitHubSourceConnector
https://github.com/simplesteph/kafka-connect-github-source/blob/master/src/main/java/com/simplesteph/kafka/GitHubSourceConnectorConfig.java

9.Create equivalent to mongo shell like queries
http://pingax.com/trick-convert-mongo-shell-query-equivalent-java-objects/

10. Article on features of JDBC Connector in details, maybe implement similar?
https://www.confluent.io/blog/kafka-connect-deep-dive-jdbc-source-connector

11. Create custom sink Connector
https://hackernoon.com/writing-your-own-sink-connector-for-your-kafka-stack-fa7a7bc201ea

12. Dev Guide from confluent
https://docs.confluent.io/2.0.0/connect/devguide.html

13. Kafka Connect documentation
https://docs.confluent.io/current/connect/javadocs/org/apache/kafka/connect/connector/Connector.html

14. Debezium Mongodb Source connector tutorial
https://medium.com/tech-that-works/cloud-kafka-connector-for-mongodb-source-8b525b779772

15. Catch moving data with modofied Debezium
https://medium.com/@vinee.27.10/catch-the-moving-data-e2b281ff2813

16. Debezium chat
https://gitter.im/debezium/user

17. Kafka contacts
https://kafka.apache.org/contact

18. ---PRESTO
Presto is Connector to MongoDb which allows to query from Mongo as SQL.



Error handling:
1. Was not given incrementing column, or was given wrong one
2. Query syntax is not correct
3. in "Timestamp" mode, column has to be type of Timestamp


# quickstart Kafka

start zookeeper
bin/zookeeper-server-start etc/kafka/zookeeper.properties

start kafka
./bin/kafka-server-start ./etc/kafka/server.properties

start Schema Registry
bin/schema-registry-start etc/schema-registry/schema-registry.properties

start producer
bin/kafka-avro-console-producer --broker-list localhost:9092 --topic test
bin/kafka-console-producer --broker-list localhost:9092 --topic test

start consumer
bin/kafka-avro-console-consumer --bootstrap-server localhost:9092 --topic transaction_items
bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic test --from-beginning
