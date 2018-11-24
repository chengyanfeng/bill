package src;


import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.ExcelUtil;
import util.Mongodbjdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class bill_xh {
    private static final Logger logger = LoggerFactory
            .getLogger(bill_xh.class);
    int flag = 1;

    /**
     * 计算总计数据
     *
     * @param startTime        开始时间
     * @param endTime          结束时间
     * @param filePath         excel文件路径
     * @param defaultSheetName 默认工作表名
     */
    public void calculateCC(String startTime, String endTime, String filePath, String defaultSheetName) {
        MongoCursor<Document> iterator = null;
        try {
            iterator = this.queryData(startTime, endTime);
            this.exportData(filePath, defaultSheetName, iterator, startTime, endTime);
        } catch (Exception e) {
            logger.error(e.toString());
            e.printStackTrace();

        } finally {
            if (null != iterator) {
                iterator.close();
            }
        }
    }


    /**
     * 查询数据
     *
     * @param startTime 开始时间
     * @param endTime   结束时间
     * @return 查询结果
     */
    private MongoCursor<Document> queryData(String startTime, String endTime) {
        List<Document> pipeline = new ArrayList<>();

//        筛选条件
        Document match = new Document("begin_time", new Document("$gte", startTime))
                .append("begin_time", new Document("$lte", endTime));
        pipeline.add(new Document("$match", match));

//        返回字段
        Document project = new Document("account", 1)
                .append("call_duration", 1)
                .append("platformType", 1)
                .append("minutes", 1);
        pipeline.add(new Document("$project", project));

//        分组条件
        Document groupBy = new Document("_id", new Document("account", "$account")
                .append("platformType", "$platformType"))
                .append("totalCount", new Document("$sum", 1))
                .append("totalSeconds", new Document("$sum", "$call_duration"))
                .append("totalMinutes", new Document("$sum", "$minutes"));
        pipeline.add(new Document("$group", groupBy));

        MongoCollection<Document> collection = Mongodbjdbc.MongGetDom().getCollection("bill_yiketong_xiaohao_cdr_query");
        return collection.aggregate(pipeline).allowDiskUse(true).iterator();
    }

    /**
     * 导出数据到excel
     *
     * @param filePath         excel文件路径
     * @param defaultSheetName 默认工作表名
     * @param iterator         数据
     */
    private void exportData(String filePath, String defaultSheetName, MongoCursor<Document> iterator, String startTime, String endTime) {
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
        headers.put(3, "计费秒数");
        headers.put(4, "计费费用（元）");
        ExcelUtil.INSTANCE.insertRows(sheet, ExcelUtil.INSTANCE.getNextRowNum(), headers);

        while (iterator.hasNext()) {
            Document document = iterator.next();
            String account = document.get("_id", Document.class).getString("account");
            String totalCount = String.valueOf(document.getInteger("totalCount"));
            String totalMinutes = "";
            try {
                totalMinutes = String.valueOf(document.getInteger("totalMinutes"));

            } catch (Exception e) {
                totalMinutes = String.valueOf(document.getLong("totalMinutes"));
            }
            String totalSeconds = String.valueOf(document.getInteger("totalSeconds"));
            Map<Integer, String> rowCells = new HashMap<>();
            rowCells.put(0, account);
            rowCells.put(1, totalCount);
            rowCells.put(2, totalMinutes);
            rowCells.put(3, totalSeconds);
            rowCells.put(4, String.format("%.2f", this.calculateTotalPrice(document, startTime, endTime)) + "");
            ExcelUtil.INSTANCE.insertRows(sheet, ExcelUtil.INSTANCE.getNextRowNum(), rowCells);

        }
        ExcelUtil.INSTANCE.saveExcelAndReset(wb, ExcelUtil.INSTANCE.getFilePath());
    }

    /**
     * 计算总价
     *
     * @param
     * @return 总价
     */
    private double calculateTotalPrice(Document document, String startTime, String endTime) {
        String accountId = document.get("_id", Document.class).getString("account");
        MongoCollection<Document> collection_cc = Mongodbjdbc.MongGetDom().getCollection("bill_yiketong_xiaohao_cdr_query");
        MongoCursor<Document> iterator = getIterator(collection_cc, accountId, startTime, endTime);
        MongoCollection<Document> collection = Mongodbjdbc.MongGetDom().getCollection("platform_account_product");
        Document data = collection.find(new Document("_id", accountId + "_cc")).first();
        double total = 0;
        long price = 1000;
        long cost = 250;
        while (iterator.hasNext()) {
            flag++;
            if(flag%10000==0){
                System.out.println("计费----------小号----------->第"+flag+"条数据");
            }
            Document document1 = iterator.next();
            String platformType = document1.getString("platformType");
            Boolean ifCallback = document1.getBoolean("ifCallback");
            long Minutes = 0;
            try {
                Minutes = document1.getInteger("minutes");
            } catch (Exception e) {
                try {
                    Minutes = document1.getLong("minutes");
                } catch (Exception a) {
                    Minutes = 0;
                }
            }
            if (platformType == null) {
                platformType = "";
            }
            if (platformType.equals("TY")) {
                price = 1500;
                cost = 240;

            } else if (platformType.equals("XZ")) {
                price = 1500;
                cost = 350;
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
            total = total + Minutes * price / 10000.0;
        }
        return total;
    }

    private MongoCursor<Document> getIterator(MongoCollection<Document> collection, String accountId, String
            startTime, String endTime) {
        FindIterable<Document> documents = collection.find(new Document().append("begin_time", new Document()
                .append("$gte", startTime))
                .append("begin_time", new Document().append("$lte", endTime)).append("account", accountId)).noCursorTimeout(true);
        MongoCursor<Document> iterator = documents.iterator();
        return iterator;
    }


}