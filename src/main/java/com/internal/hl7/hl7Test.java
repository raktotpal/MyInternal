package com.internal.hl7;

import org.apache.commons.lang3.StringUtils;

//import ca.uhn.hl7v2.DefaultHapiContext;
//import ca.uhn.hl7v2.HL7Exception;
//import ca.uhn.hl7v2.HapiContext;
//import ca.uhn.hl7v2.model.Message;
//import ca.uhn.hl7v2.parser.CanonicalModelClassFactory;
//import ca.uhn.hl7v2.parser.Parser;
//import ca.uhn.hl7v2.parser.PipeParser;

//import com.zaloni.HL7.mr.parser.HL7Validator;

@SuppressWarnings("unused")
public class hl7Test {

  public static void main(String[] args) { // throws HL7Exception {
  // String hlvRecord =
  // "MSH|^~\\&|GHH LAB|ELAB-3|GHH OE|BLDG4|200202150930||ORU^R01|CNTRL-3456|P|2.4\r"
  // +
  // "PID|||555-44-4444||EVERYWOMAN^EVE^E^^^^L|JONES|19620320|F|||153 FERNWOOD DR.^STATESVILLE^OH^35292||(206)334-5232|(206)752-1201||||AC555444444||67-A4335^OH^20030520\r"
  // +
  // "OBR|1|845439^GHH OE|1045813^GHH LAB|15545^GLUCOSE|||200202150730|||||||||555-55-5555^PRIMARY^PATRICIA P^^^^MD^^|||||||||F||||||444-44-4444^HIPPOCRATES^HOWARD H^^^^MD\r"
  // +
  // "OBX|1|SN|1554-5^GLUCOSE^POST 12H CFST:MCNC:PT:SER/PLAS:QN||^182|mg/dl|70_105|H|||F\r";
  //
  // HL7Validator hl7Validator = new HL7Validator();
  //
  // CanonicalModelClassFactory cms = new CanonicalModelClassFactory();
  // @SuppressWarnings("resource")
  // HapiContext hapiContext = new DefaultHapiContext();
  // cms = new CanonicalModelClassFactory("2.4");
  //
  // hapiContext.setModelClassFactory(cms);
  //
  // Message hapiMsg = null;
  // Parser parser = new PipeParser();
  // hapiMsg = parser.parse(hlvRecord);
  //
  // String[] messageInfo = new String[3];
  // messageInfo = getVersion(hlvRecord);
  // messageInfo[2] = "MSH|^~\\&";
  //
  // hl7Validator.isValid(hapiMsg, messageInfo);

  }

  private static String[] getVersion(String hl7Message) throws IndexOutOfBoundsException {
    String[] messageInfo = new String[3];

    // 12th position of a MSH is version, But as ARRAY is zero-indexed, we are
    // extracting 11th object.
    messageInfo[0] = StringUtils.splitByWholeSeparatorPreserveAllTokens(hl7Message, "|")[11]
        .split("\\s")[0];

    messageInfo[1] = StringUtils.splitByWholeSeparatorPreserveAllTokens(hl7Message, "|")[8]
        .split("\\s")[0];

    messageInfo[2] = StringUtils.EMPTY;

    StringBuffer debugContent = new StringBuffer();
    debugContent.append("The HL7 message to be parsed is ::")
        .append(System.getProperty("line.separator")).append("--------------------------------")
        .append(System.getProperty("line.separator")).append(hl7Message)
        .append(System.getProperty("line.separator")).append("--------------------------------");
    System.out.println(debugContent.toString());
    System.out.println("The HL7 message version is :: " + messageInfo[0].trim());
    System.out.println("The HL7 message type is :: " + messageInfo[1].trim());
    return messageInfo;
  }

}
