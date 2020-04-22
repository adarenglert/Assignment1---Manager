package Manager;

public class Job{
    private String action;
    private String url;
    private int packageId;
    public Job(String action, String url, int packageId) {
        this.action = action;
        this.url = url;
        this.packageId = packageId;
    }

    public static Job buildFromMessage(String fromMessage){
        String[] data = fromMessage.split("#");
        if(data.length!=3) throw new StringIndexOutOfBoundsException;
        return new Job(data[0],data[1],Integer.parseInt(data[2]));
    }

    public int getPackageId() {
        return packageId;
    }

    public void setPackageId(int packageId) {
        this.packageId = packageId;
    }

    public String getAction() {
        return action;
    }

    public void setAction(String action) {
        this.action = action;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    @Override
    public String toString() {
        return action + '#' + url + '#' + packageId;
    }
}