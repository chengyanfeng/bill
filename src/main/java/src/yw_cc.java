package src;

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

import bean.*;

public class yw_cc {

    HashMap<String, bean> map = new HashMap<>();
    static long i = 0;

    public void calculateCC(String startTime, String endTime, String filePath, String defaultSheetName) {

        FindIterable<Document> test = Mongodbjdbc.MongGetDom().getCollection("c5_call_sheet").find(new Document().append("BEGIN_TIME", new Document()
                .append("$gte", startTime)
                .append("$lte", endTime))
                .append("CONNECT_TYPE", "dialout")
                .append("CALL_TIME_LENGTH", new Document("$gt", 0)).append("MID_NO",new Document("$exists",false))).noCursorTimeout(true)
                .projection(new Document("ACCOUNT_ID", 1)
                        .append("CALLED_NO", 1)
                        .append("CALL_NO", 1)
                        .append("CALL_TIME_LENGTH", 1)
                        .append("DISTRICT_CODE", 1));
        MongoCursor<Document> mongoCursor = test.iterator();
        //获取单价表集合
        MongoCollection<Document> collection = Mongodbjdbc.MongGetDom().getCollection("platform_account_product");
        //获取电话区号集合
        MongoCollection<Document> mobile_code_collection = Mongodbjdbc.MongGetDom().getCollection("tbl_mobile_code");
        while (mongoCursor.hasNext()) {
            bean bean = new bean();
            Document mongodb = mongoCursor.next();
            //获取参数
            String accountId = mongodb.getString("ACCOUNT_ID");
            String CALLED_NO = mongodb.getString("CALLED_NO");
            String CAll_NO = mongodb.getString("CALL_NO");
            //排除400号码
            if(CAll_NO.substring(0,1).equals("4")){
                continue;
            }
            String DISTRICT_CODE = mongodb.getString("DISTRICT_CODE");
            Integer seconds = mongodb.getInteger("CALL_TIME_LENGTH");
            long seconds6 = new Double(Math.ceil(seconds / 6.0)).longValue();
            long minutes = new Double(Math.ceil(seconds / 60.0)).longValue();

            //设置bean参数
            bean.setAccountId(accountId);
            bean.setMinutes(minutes);
            bean.setSecnods6(seconds6);
            bean.setSeconds(seconds);
            //获取价格
            Document data = collection.find(new Document("_id", accountId + "_cc")).first();
            double price = getPrcie(data, CAll_NO, CALLED_NO, DISTRICT_CODE, seconds, mobile_code_collection);
            bean.setTotalPrice(price);
            //数据分组聚合
            setMap(accountId, bean);
            i++;
            if (i % 10000 == 0) {
                System.out.println("业务----------外呼-------cc------->第" + i + "条数据");
            }
        }
        //导出数据
        exportData(filePath, defaultSheetName);


    }

    /**
     * @param data
     * @param CODE_NO
     * @param CODED_NO
     * @param DISTRICT_CODE
     * @param seconds
     * @return 获取价格
     */
    public static double getPrcie(Document data, String CODE_NO, String CODED_NO, String DISTRICT_CODE, int seconds, MongoCollection<Document> mobile_code_collection) {
        double total = 0;
        long seconds6 = new Double(Math.ceil(seconds / 6.0)).longValue();
        long minutes = new Double(Math.ceil(seconds / 60.0)).longValue();
        if (null != data) {
            String strategyType = "dialFeeStrategy";
            String feeType = "dialFee";
            String strategy = data.getString(strategyType);
            Document dialFee = data.get(feeType, Document.class);

            boolean isLocal = false;
            isLocal = isLoction(CODED_NO, DISTRICT_CODE, mobile_code_collection);
            if ("minute".equals(strategy)) {
                Integer local = dialFee.getInteger("local");
                Integer remote = dialFee.getInteger("remote");
                Integer localTelPrice = dialFee.getInteger("localTel");
                Integer remoteTelPrice = dialFee.getInteger("remoteTel");
                if (isLocal) {
                    if (isMobileNo(CODE_NO)) {
                        total = total + local * minutes / 10000.0;
                    } else {
                        total = total + localTelPrice * minutes / 10000.0;
                    }
                } else {
                    if (isMobileNo(CODE_NO)) {
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
            value.setCount(value.getCount() + 1);
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
        Set<String> keys = map.keySet();
        for (String key : keys) {
            bean insertExcel = map.get(key);
            Map<Integer, String> rowCells = new HashMap<>();
            rowCells.put(0, insertExcel.getAccountId());
            rowCells.put(1, insertExcel.getCount() + "");
            rowCells.put(2, insertExcel.getMinutes() + "");
            rowCells.put(3, insertExcel.getSecnods6() + "");
            rowCells.put(4, insertExcel.getSeconds() + "");
            rowCells.put(5, insertExcel.getTotalPrice() + "");
            ExcelUtil.INSTANCE.insertRows(sheet, ExcelUtil.INSTANCE.getNextRowNum(), rowCells);
        }

        ExcelUtil.INSTANCE.saveExcelAndReset(wb, ExcelUtil.INSTANCE.getFilePath());
    }

    private static boolean isLoction(String CODED_NO, String DISTRICT_CODE, MongoCollection<Document> mobile_code_collection) {
        if(CODED_NO.length()<8){
            return true;
        }
        if(CODED_NO.substring(0, 1).equals("4")){
            return true;
        }
        if (CODED_NO.substring(0, 1).equals("0")) {
            CODED_NO = CODED_NO.substring(0, 4);
            if (DISTRICT_CODE.equals(CODED_NO)) {
                return true;
            } else {
                return false;
            }

        } else {
            CODED_NO = CODED_NO.substring(0, 7);

            Document data = mobile_code_collection.find(new Document("_id", CODED_NO)).projection(new Document("area_code", 1)).first();
            String area_code = "";
            try {
                area_code = data.getString("area_code");
            } catch (Exception e) {
                System.out.println(e);
            }
            if (DISTRICT_CODE.equals(area_code)) {
                return true;
            } else {
                return false;
            }
        }

    }
}
