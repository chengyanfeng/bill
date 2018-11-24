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

public class yw_xh {
    HashMap<String, bean> map = new HashMap<>();
    static long i = 0;

    public void calculateCC(String startTime, String endTime, String filePath, String defaultSheetName) {

        FindIterable<Document> test = Mongodbjdbc.MongGetDom().getCollection("c5_call_sheet").find(new Document().append("BEGIN_TIME", new Document()
                .append("$gte", startTime)
                .append("$lte", endTime))
                .append("CALL_TIME_LENGTH", new Document("$gt", 0)).append("MID_NO",new Document("$exists",true))).noCursorTimeout(true)
                .projection(new Document("ACCOUNT_ID", 1)
                        .append("CONNECT_TYPE", 1)
                        .append("CALL_TIME_LENGTH", 1));
        MongoCursor<Document> mongoCursor = test.iterator();
        //获取单价表集合
        MongoCollection<Document> collection = Mongodbjdbc.MongGetDom().getCollection("platform_account_product");
        while (mongoCursor.hasNext()) {
            bean bean = new bean();
            Document mongodb = mongoCursor.next();
            //获取参数
            String accountId = mongodb.getString("ACCOUNT_ID");
            String CONNECT_TYPE = mongodb.getString("CONNECT_TYPE");
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
            double price = getPrcie(data, CONNECT_TYPE,seconds );
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
     *
     * @param data
     * @param CONNECT_TYPE
     * @param CALL_TIME_LENTH
     * @return
     */

    public static double getPrcie(Document data, String CONNECT_TYPE, long CALL_TIME_LENTH) {
        double total = 0;
        long price = 1000;
        Boolean ifCallback = true;
        if (CONNECT_TYPE.equals("dialout")) {
            ifCallback = false;
        }
        long Minutes = 0;
        long minutes = new Double(Math.ceil(CALL_TIME_LENTH / 60.0)).longValue();
        if (null != data) {
            Object strategy = data.get("yiketongxiaohaoChat");
            if (strategy != null) {
                Object yiketongxiaohaoChat = data.get("yiketongxiaohaoChat");
                Integer yiketongxiaohaoPrice = ((Document) yiketongxiaohaoChat).getInteger("yiketongxiahaoPrice");
                Integer yiketongxiahaoCallbackPrice = ((Document) yiketongxiaohaoChat).getInteger("yiketongxiahaoCallbackPrice");
                if (ifCallback != null) {
                    if (ifCallback) {
                        if (yiketongxiahaoCallbackPrice != null) {
                            price = (Double.valueOf(String.valueOf(yiketongxiahaoCallbackPrice))).longValue();
                        } else {
                            price = 500;
                        }
                    } else {
                        if (yiketongxiaohaoPrice != null) {
                            price = (Double.valueOf(String.valueOf(yiketongxiaohaoPrice))).longValue();
                        }


                    }
                }
             total=minutes*price/10000.0;
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
        map.clear();
    }

    private static boolean isLoction(String CODED_NO, String DISTRICT_CODE, MongoCollection<Document> mobile_code_collection) {
        if(CODED_NO.length()<8){
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
                System.out.println("-------------------------------->"+CODED_NO);
                System.out.println("业务------小号------区域对比------area_code---为空");
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
