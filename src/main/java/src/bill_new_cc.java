package src;

import bean.bean;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.bson.Document;
import util.ExcelUtil;
import util.Mongodbjdbc;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class bill_new_cc {

        HashMap<String, bean> map = new HashMap<>();
        long i = 0;
        public void calculateCC(String startTime, String endTime, String filePath, String defaultSheetName) {

            FindIterable<Document> test = Mongodbjdbc.MongGetDom().getCollection(
                    "bill_cdr_query_cc").find(new Document().append("beginTime", new Document()
                    .append("$gte", startTime)
                    .append("$lte", endTime)))
                    .projection(new Document().append("account",1).append("did",1).append("callerDistrictNo",1).append("calleeDistrictNo",1).append("seconds",1).append("seconds6",1).append("minutes",1))
                    .noCursorTimeout(true);
            MongoCursor<Document> mongoCursor = test.iterator();
            MongoCollection<Document> collection = Mongodbjdbc.MongGetDom().getCollection("platform_account_product");
            while (mongoCursor.hasNext()) {
                bean bean = new bean();
                Document mongodb = mongoCursor.next();
                //获取参数
                String accountId = mongodb.getString("account");
                String did = mongodb.getString("did");
                String callerDistrictNo = mongodb.getString("callerDistrictNo");
                String calleeDistrictNo = mongodb.getString("calleeDistrictNo");
                Long seconds = mongodb.getLong("seconds");
                Long seconds6 = mongodb.getLong("seconds6");
                Long minutes = mongodb.getLong("minutes");

                //设置bean参数
                bean.setAccountId(accountId);
                bean.setMinutes(minutes);
                bean.setSecnods6(seconds6);
                bean.setSeconds(seconds);
                //获取价格
                Document data = collection.find(new Document("_id", accountId + "_cc")).first();
                double price = getPrcie(data, did, seconds,callerDistrictNo,calleeDistrictNo);
                bean.setTotalPrice(price);
                //数据分组聚合
                setMap(accountId, bean);
                i++;
                if(i%10000==0){
                    System.out.println("计费----------外呼-------cc------->第"+i+"条数据");
                }
            }
            //导出数据
            exportData(filePath,defaultSheetName);


        }

        public static double getPrcie(Document data, String number, long seconds,String callerDistrictNo,String calleeDistrictNo) {
            double total = 0;
            long seconds6 = new Double(Math.ceil(seconds / 6.0)).longValue();
            long minutes = new Double(Math.ceil(seconds / 60.0)).longValue();
            if (null != data) {
                String strategyType = "dialFeeStrategy";
                String feeType = "dialFee";
                String strategy = data.getString(strategyType);
                Document dialFee = data.get(feeType, Document.class);
                boolean isLocal = false;
            if (callerDistrictNo.equals(calleeDistrictNo)) {
                isLocal = true;
            }
                if ("minute".equals(strategy)) {
                    Integer local = dialFee.getInteger("local");
                    Integer remote = dialFee.getInteger("remote");
                    Integer localTelPrice = dialFee.getInteger("localTel");
                    Integer remoteTelPrice = dialFee.getInteger("remoteTel");
                    if (isLocal) {
                        if (isMobileNo(number)) {
                            total = total + local * minutes / 10000.0;
                        } else {
                            total = total + localTelPrice * minutes / 10000.0;
                        }
                    } else {
                        if (isMobileNo(number)) {
                            total = total + remote * minutes / 10000.0;
                        } else {
                            total = total + remoteTelPrice * minutes / 10000.0;
                        }
                    }
                } else if ("minute3".equals(strategy)) {
                    Integer remotePrice = dialFee.getInteger("remote");
                    Integer localPrice = dialFee.getInteger("local");
                    Integer prefixMin = dialFee.getInteger("prefixMin");
                    if (isLocal) {
                        long afterTotalMinutes = minutes - 3;
                        if (afterTotalMinutes < 0) {
                            afterTotalMinutes = 0;
                        }
                        total = total + prefixMin / 10000.0 + afterTotalMinutes * localPrice / 10000.0;
                    } else {
                        total = total + remotePrice * minutes / 10000.0;
                    }
                } else if ("second6".equals(strategy)) {
                    Integer remotePrice = dialFee.getInteger("remote");
                    Integer localPrice = dialFee.getInteger("local");
                    // 市话
                    if (isLocal) {
                        total = total + localPrice * seconds6 / 10000.0;
                    }
                    // 长途
                    else {
                        total = total + remotePrice * seconds6 / 10000.0;
                    }
                } else if ("minute3s6".equals(strategy)) {
                    Integer remotePrice = dialFee.getInteger("remote");
                    Integer localPrice = dialFee.getInteger("local");
                    Integer prefixMin = dialFee.getInteger("prefixMin");
                    // 市话
                    if (isLocal) {
                        long after3m = minutes - 3;
                        if (after3m < 0) {
                            after3m = 0;
                        }
                        total = total + prefixMin / 10000.0 + after3m * localPrice / 10000.0;
                    }
                    // 长途
                    else {
                        total = total + remotePrice * seconds6 / 10000.0;

                    }
                }

            }
            return total;
        }

        /**
         * 判断是否手机号
         *
         * @param number 待检验数据
         * @return true：是手机号，false：不是手机号
         */
        private static boolean isMobileNo(String number) {
            if (number.matches("1[34578][0-9]{9}"))
                return true;
            return false;
        }

        /**
         * 把数据合并到map中
         */
        private void setMap(String accountId, bean bean) {
            bean value = map.get(accountId);
            if (value != null) {
                value.setSecnods6(value.getSecnods6() + bean.getSecnods6());
                value.setSeconds(value.getSeconds() + bean.getSeconds());
                value.setMinutes(value.getMinutes() + bean.getMinutes());
                value.setTotalPrice(value.getTotalPrice() + bean.getTotalPrice());
                value.setCount(value.getCount()+1);
            } else {
                bean.setCount(1);
                map.put(accountId, bean);
            }
        }


        /**
         * 导出数据到excel
         *
         * @param filePath         excel文件路径
         * @param defaultSheetName 默认工作表名
         *
         */
        private void exportData(String filePath, String defaultSheetName) {
//        创建工作簿和表
            SXSSFWorkbook wb = ExcelUtil.INSTANCE.returnWorkBookGivenFileHandle(filePath, defaultSheetName);
            if (null == wb) {
                return;
            }

//        将列头添加至表
            SXSSFSheet sheet = ExcelUtil.INSTANCE.returnSheetFromWorkBook(wb);
            Map<Integer, String> headers = new HashMap<>();
            headers.put(0, "账户编号");
            headers.put(1, "条数");
            headers.put(2, "计费分钟");
            headers.put(3, "计费6秒数");
            headers.put(4, "计费秒数");
            headers.put(5, "计费费用（元）");
            ExcelUtil.INSTANCE.insertRows(sheet, ExcelUtil.INSTANCE.getNextRowNum(), headers);
            Set<String> keys= map.keySet();
            for(String key: keys) {
                bean insertExcel=map.get(key);
                Map<Integer, String> rowCells = new HashMap<>();
                rowCells.put(0, insertExcel.getAccountId());
                rowCells.put(1, insertExcel.getCount()+"");
                rowCells.put(2, insertExcel.getMinutes()+"");
                rowCells.put(3, insertExcel.getSecnods6()+"");
                rowCells.put(4, insertExcel.getSeconds()+"");
                rowCells.put(5, insertExcel.getTotalPrice()+"");
                ExcelUtil.INSTANCE.insertRows(sheet, ExcelUtil.INSTANCE.getNextRowNum(), rowCells);
            }

            ExcelUtil.INSTANCE.saveExcelAndReset(wb, ExcelUtil.INSTANCE.getFilePath());
        }




}
