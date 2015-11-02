package pipeline.test.doubleData;

/**
 * Created by root on 10/7/15.
 */
public class Data {

	private String regex;
	private String label;
	private String defaultLabel;

	public Data(String r, String l, String def){
		this.regex  = r;
		this.label = l;
		this.defaultLabel = def;
	}

	public String getRegex() {
		return regex;
	}

	public void setRegex(String regex) {
		this.regex = regex;
	}

	public String getLabel() {
		return label;
	}

	public void setLabel(String label) {
		this.label = label;
	}

	public String getDefaultLabel() {
		return defaultLabel;
	}

	public void setDefaultLabel(String defaultLabel) {
		this.defaultLabel = defaultLabel;
	}
}
