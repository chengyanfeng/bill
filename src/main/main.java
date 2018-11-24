package main;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import src.*;

import java.io.InputStream;
import java.util.Properties;
import java.io.IOException;

public class main {

    private static final Logger logger = LoggerFactory.getLogger(main.class);

    public static void main(String arg[]) {
        String path = getProperties_3("/main/resources/path.properties", "path");
        System.out.println("配置文件路径:"+path);
        //计费---小号----数据
       new Thread() {
            public void run() {
                System.out.println("启动------计费-------小号---------线程");
                logger.info("启动------计费-------小号---------线程");
                bill_xh xh = new bill_xh();
                xh.calculateCC("2017-11-01 00:00:00", "2018-12-01 00:00:00", path  + "2017-11-01_bill_xh.xlsx", "sheet");
                xh.calculateCC("2018-03-01 00:00:00", "2018-04-01 00:00:00", path + "2018-03-01_bill_xh.xlsx", "sheet");
                xh.calculateCC("2018-09-01 00:00:00", "2018-10-01 00:00:00", path + "2018-09-01_bill_xh.xlsx", "sheet");
            }
        }.start();
        //计费---外呼---cc--数据
        new Thread() {
            public void run() {
                System.out.println("启动--------计费-----外呼-------cc----------线程");
                logger.info("启动-------计费------外呼-------cc-----------线程");
                bill_new_cc new_cc = new bill_new_cc();
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------计费------外呼-------cc----2016-06-01-------线程");
                        logger.info("启动-------计费------外呼-------cc----2016-06-01-------线程");
                        new_cc.calculateCC("1464710400", "1467302399", path + "2016-06-01-bill-cc.xlsx", "sheet");
                    }
                }.start();
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------计费------外呼-------cc----2016-12-01-------线程");
                        logger.info("启动-------计费------外呼-------cc----2016-12-01-------线程");
                        new_cc.calculateCC("1480521600", "1483200000", path + "2016-12-01-bill-cc.xlsx", "sheet");
                    }
                }.start();
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------计费------外呼-------cc----2017-03-01-------线程");
                        logger.info("启动-------计费------外呼-------cc----2017-03-01-------线程");
                        new_cc.calculateCC("1490976000", "1493568000", path + "2017-04-01-bill-cc.xlsx", "sheet");
                    }
                }.start();

                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------计费------外呼-------cc----2017-11-01-------线程");
                        logger.info("启动-------计费------外呼-------cc----2017-11-01-------线程");
                        new_cc.calculateCC("1509465600", "1512057600", path + "2017-11-01-bill-cc.xlsx", "sheet");
                    }
                }.start();

                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------计费------外呼-------cc----2018-03-01-------线程");
                        logger.info("启动-------计费------外呼-------cc----2018-03-01-------线程");
                        new_cc.calculateCC("1519833600", "1522512000", path + "2018-03-01-bill-cc.xlsx", "sheet");
                    }
                }.start();


                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------计费------外呼-------cc----2018-09-01-------线程");
                        logger.info("启动-------计费------外呼-------cc----2018-09-01-------线程");
                        new_cc.calculateCC("1535731200", "1538323200", path + "2018-09-01-bill-cc.xlsx", "sheet");
                    }
                }.start();

            }
        }.start();
        //业务----外呼-----cc-----数据
        new Thread() {
            public void run() {
                System.out.println("启动-------业务------外呼-------cc-----------线程");
                logger.info("启动-------业务------外呼-------cc-----------线程");
                yw_cc yw_cc = new yw_cc();
               //开启内部线程
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------业务------外呼-------cc----2016-06-01-------线程");
                        logger.info("启动-------业务------外呼-------cc----2016-06-01-------线程");
                        yw_cc.calculateCC("2016-06-01 00:00:00", "2016-07-01 00:00:00", path + "2016-06-01-yw-cc.xlsx", "sheet");
                    }
                }.start();
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------业务------外呼-------cc----2016-12-01-------线程");
                        logger.info("启动-------业务------外呼-------cc----2016-12-01-------线程");
                        yw_cc.calculateCC("2016-12-01 00:00:00", "2017-01-01 00:00:00", path + "2016-12-01-yw-cc.xlsx", "sheet");
                    }
                }.start();
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------业务------外呼-------cc----2017-04-01-------线程");
                        logger.info("启动-------业务------外呼-------cc----2017-04-01-------线程");
                        yw_cc.calculateCC("2017-04-01 00:00:00", "2017-05-01 00:00:00", path + "2017-04-01-yw-cc.xlsx", "sheet");
                        }
                }.start();
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------业务------外呼-------cc----2017-11-01-------线程");
                        logger.info("启动-------业务------外呼-------cc----2017-11-01-------线程");
                        yw_cc.calculateCC("2017-11-01 00:00:00", "2017-12-01 00:00:00", path + "2017-11-01-yw-cc.xlsx", "sheet");
                    }
                }.start();
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------业务------外呼-------cc----2018-03-01-------线程");
                        logger.info("启动-------业务------外呼-------cc----2018-03-01-------线程");
                        yw_cc.calculateCC("2018-03-01 00:00:00", "2018-04-01 00:00:00", path + "2018-03-01-yw-cc.xlsx", "sheet");
                    }
                }.start();
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------业务------外呼-------cc----2018-09-01-------线程");
                        logger.info("启动-------业务------外呼-------cc----2018-09-01-------线程");
                        yw_cc.calculateCC("2018-09-01 00:00:00", "2018-10-01 00:00:00", path + "2018-09-01-yw-cc.xlsx", "sheet");
                    }
                }.start();

            }
        }.start();

        //业务----小号-----xh-----数据
        new Thread() {
            public void run() {
                System.out.println("启动------业务-------小号---------线程");
                logger.info("启动------业务-------小号---------线程");
                yw_xh yw_xh = new yw_xh();

                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------业务------外呼-------小号----2016-06-01-------线程");
                        logger.info("启动-------业务------外呼-------小号----2016-06-01-------线程");
                        yw_xh.calculateCC("2016-06-01 00:00:00", "2016-07-01 00:00:00", path + "2016-06-01-yw-xh.xlsx", "sheet");
                    }
                }.start();
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------业务------外呼-------小号----2016-12-01-------线程");
                        logger.info("启动-------业务------外呼-------小号----2016-12-01-------线程");
                        yw_xh.calculateCC("2016-12-01 00:00:00", "2017-01-01 00:00:00", path + "2016-12-01-yw-xh.xlsx", "sheet");
                    }
                }.start();
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------业务------外呼-------小号----2017-03-01-------线程");
                        logger.info("启动-------业务------外呼-------小号----2017-03-01-------线程");
                        yw_xh.calculateCC("2017-04-01 00:00:00", "2017-05-01 00:00:00", path + "2017-03-01-yw-xh.xlsx", "sheet");
                    }
                }.start();
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------业务------外呼-------小号----2017-11-01-------线程");
                        logger.info("启动-------业务------外呼-------小号----2017-11-01-------线程");
                        yw_xh.calculateCC("2017-11-01 00:00:00", "2017-11-01 00:00:00", path + "2017-11-01-yw-xh.xlsx", "sheet");
                    }
                }.start();
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------业务------外呼-------小号----2018-03-01-------线程");
                        logger.info("启动-------业务------外呼-------小号----2018-03-01-------线程");
                        yw_xh.calculateCC("2018-03-01 00:00:00", "2018-04-01 00:00:00", path + "2018-03-01-yw-xh.xlsx", "sheet");
                    }
                }.start();
                new Thread() {
                    @Override
                    public void run() {
                        System.out.println("启动-------业务------外呼-------小号----2018-09-01-------线程");
                        logger.info("启动-------业务------外呼-------小号----2018-09-01-------线程");
                        yw_xh.calculateCC("2018-09-01 00:00:00", "2018-10-01 00:00:00", path + "2018-09-01-yw-xh.xlsx", "sheet");
                    }
                }.start();


            }
        }.start();


    }

    public static String getProperties_3(String filePath, String keyWord) {
        Properties prop = new Properties();
        String value = null;
        try {
            InputStream inputStream = main.class.getResourceAsStream(filePath);
            prop.load(inputStream);
            value = prop.getProperty(keyWord);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return value;
    }
}
