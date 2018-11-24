package bean;

public class bean {
    private String accountId;
    private long count;
    private long seconds;
    private long minutes;
    private long secnods6;
    private double totalPrice;

    public long getSeconds() {
        return seconds;
    }

    public void setSeconds(long seconds) {
        this.seconds = seconds;
    }

    public long getMinutes() {
        return minutes;
    }

    public void setMinutes(long minutes) {
        this.minutes = minutes;
    }

    public long getSecnods6() {
        return secnods6;
    }

    public void setSecnods6(long secnods6) {
        this.secnods6 = secnods6;
    }

    public double getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(double totalPrice) {
        this.totalPrice = totalPrice;
    }


    public String getAccountId() {
        return accountId;
    }

    public void setAccountId(String accountId) {
        this.accountId = accountId;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "bean{" +
                "accountId='" + accountId + '\'' +
                ", count=" + count +
                ", seconds=" + seconds +
                ", minutes=" + minutes +
                ", secnods6=" + secnods6 +
                ", totalPrice=" + totalPrice +
                '}';
    }



}
