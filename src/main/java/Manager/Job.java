package Manager;

public class Job{
    private final String inputUrl;
    private String outputUrl;
    private String action;
    private int packageId;
    public Job(String action, String url, int packageId) {
        this.action = action;
        this.inputUrl = url;
        this.outputUrl = "";
        this.packageId = packageId;
    }

    public Job(String action, String url,String outputUrl, int packageId) {
        this.action = action;
        this.inputUrl = url;
        this.outputUrl = outputUrl;
        this.packageId = packageId;
    }

    public String getOutputUrl() {
        return outputUrl;
    }

    public void setOutputUrl(String outputUrl) {
        this.outputUrl = outputUrl;
    }

    public static Job buildFromMessage(String fromMessage){
        String[] data = fromMessage.split("#");
        switch (data.length){
            case 3: return new Job(data[0],data[1],Integer.parseInt(data[2]));
            case 4: return new Job(data[0],data[1],data[2],Integer.parseInt(data[3]));
            default: throw new StringIndexOutOfBoundsException();
        }

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
        return inputUrl;
    }

    @Override
    public String toString() {
        if(outputUrl.equals(""))
            return action + '#' + inputUrl + '#' + packageId;
        return action + '#' + inputUrl + '#' + outputUrl + '#' + packageId;
    }
}