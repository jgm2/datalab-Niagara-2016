package niagara.firehose;

import java.io.IOException;

import niagara.utils.PEException;

class XMLDTDGenerator extends XMLFirehoseGen {

	// private XMLGenerator xmlGenerator;
	// private String dtdName;

	public XMLDTDGenerator(String dtdName, boolean streaming,
			boolean prettyprint) {
		// this.dtdName = dtdName;
		useStreamingFormat = streaming;
		usePrettyPrint = prettyprint;
		// xmlGenerator = new XMLGenerator();
	}

	public String generateXMLString() throws IOException {
		// Document d = xmlGenerator.generateXML(dtdName);

		// convert the document to a string and return, but check
		// the documentation (which I don't have at home) to see if
		// we can get a string out of XMLGenerator
		// fix this !!!
		throw new PEException("KT - needs to be fixed");
	}
}
