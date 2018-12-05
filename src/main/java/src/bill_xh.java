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

public class bill_xh {
    HashMap<String, bean> map = new HashMap<>();
    static long i = 0;

    public void calculateCC(String startTime, String endTime, String filePath, String defaultSheetName) {

        FindIterable<Document> test = Mongodbjdbc.MongGetDom().getCollection("bill_yiketong_xiaohao_cdr_query").find(new Document().append("begin_time", new Document()
                .append("$gt", startTime)
                .append("$lt", endTime))).noCursorTimeout(true)
                .projection(new Document("account", 1)
                        .append("call_duration", 1)
                        .append("platformType", 1)
                        .append("ifCallback", 1)
                        .append("minutes",1)
                );
        MongoCursor<Document> mongoCursor = test.iterator();
        //获取单价表集合
        MongoCollection<Document> collection = Mongodbjdbc.MongGetDom().getCollection("platform_account_product");
        while (mongoCursor.hasNext()) {
            bean bean = new bean();
            Document mongodb = mongoCursor.next();
            //获取参数
            String accountId = mongodb.getString("account");
            Integer call_duration = mongodb.getInteger("call_duration");
            String platformType = mongodb.getString("platformType");
            Long minutes = mongodb.getLong("minutes");


            //设置bean参数
            bean.setAccountId(accountId);
            bean.setSeconds(call_duration);
            if (minutes!=null){
                bean.setMinutes(minutes);
                }else {
                minutes=0l;
                bean.setMinutes(0l);
            }
            //获取价格
            Document data = collection.find(new Document("_id", accountId + "_cc")).first();
            double price = getPrcie(data, mongodb, platformType,minutes);
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
     * @return
     */

    public static double getPrcie(Document data, Document mongodb, String platformType,long minutes) {
        double total = 0;
        long price = 1000;
        Boolean ifCallback=   mongodb.getBoolean("ifCallback");
        if (platformType == null) {
            platformType = "";
        }
        if (platformType.equals("TY")) {
            price = 1500;


        } else if (platformType.equals("XZ")) {
            price = 1500;

        }
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

            }

        }
        total=minutes*price/10000.0;
        return total;
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

}
