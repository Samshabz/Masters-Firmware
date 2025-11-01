#include <HardwareSerial.h>

const int PWR_ON_PIN = 15;
const int RX_PIN     = 2;
const int TX_PIN     = 1;

HardwareSerial lteSerial(1);

static const char AWS_CERT_CA[] PROGMEM = R"EOF(
-----BEGIN CERTIFICATE-----
MIIDQTCCAimgAwIBAgITBmyfz5m/jAo54vB4ikPmljZbyjANBgkqhkiG9w0BAQsF
ADA5MQswCQYDVQQGEwJVUzEPMA0GA1UEChMGQW1hem9uMRkwFwYDVQQDExBBbWF6
b24gUm9vdCBDQSAxMB4XDTE1MDUyNjAwMDAwMFoXDTM4MDExNzAwMDAwMFowOTEL
MAkGA1UEBhMCVVMxDzANBgNVBAoTBkFtYXpvbjEZMBcGA1UEAxMQQW1hem9uIFJv
b3QgQ0EgMTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALJ4gHHKeNXj
ca9HgFB0fW7Y14h29Jlo91ghYPl0hAEvrAIthtOgQ3pOsqTQNroBvo3bSMgHFzZM
9O6II8c+6zf1tRn4SWiw3te5djgdYZ6k/oI2peVKVuRF4fn9tBb6dNqcmzU5L/qw
IFAGbHrQgLKm+a/sRxmPUDgH3KKHOVj4utWp+UhnMJbulHheb4mjUcAwhmahRWa6
VOujw5H5SNz/0egwLX0tdHA114gk957EWW67c4cX8jJGKLhD+rcdqsq08p8kDi1L
93FcXmn/6pUCyziKrlA4b9v7LWIbxcceVOF34GfID5yHI9Y/QCB/IIDEgEw+OyQm
jgSubJrIqg0CAwEAAaNCMEAwDwYDVR0TAQH/BAUwAwEB/zAOBgNVHQ8BAf8EBAMC
AYYwHQYDVR0OBBYEFIQYzIU07LwMlJQuCFmcx7IQTgoIMA0GCSqGSIb3DQEBCwUA
A4IBAQCY8jdaQZChGsV2USggNiMOruYou6r4lK5IpDB/G/wkjUu0yKGX9rbxenDI
U5PMCCjjmCXPI6T53iHTfIUJrU6adTrCC2qJeHZERxhlbI1Bjjt/msv0tadQ1wUs
N+gDS63pYaACbvXy8MWy7Vu33PqUXHeeE6V/Uq2V8viTO96LXFvKWlJbYK8U90vv
o/ufQJVtMVT8QtPHRh8jrdkPSHCa2XV4cdFyQzR1bldZwgJcJmApzyMZFo6IQ6XU
5MsI+yMRQ+hDKXJioaldXgjUkK642M4UwtBV8ob2xJNDd2ZhwLnoQdeXeGADbkpy
rqXRfboQnoZsG4q5WTP468SQvvG5
-----END CERTIFICATE-----
)EOF";

static const char AWS_CERT_CLIENT[] PROGMEM = R"EOF(
-----BEGIN CERTIFICATE-----
MIIDWTCCAkGgAwIBAgIUVIrldKces8SfoLsN5aMZksWJhm4wDQYJKoZIhvcNAQEL
BQAwTTFLMEkGA1UECwxCQW1hem9uIFdlYiBTZXJ2aWNlcyBPPUFtYXpvbi5jb20g
SW5jLiBMPVNlYXR0bGUgU1Q9V2FzaGluZ3RvbiBDPVVTMB4XDTI1MDMyNDE1MTQz
NFoXDTQ5MTIzMTIzNTk1OVowHjEcMBoGA1UEAwwTQVdTIElvVCBDZXJ0aWZpY2F0
ZTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAJufRsekvkqz7kX9XiUF
phnQ2bh1McNBUYmM3gSKVahrCvz7KRQNeywmY55IejRBmoP+xkSjmVlu3rmDRKVs
ootpj0QMYO+MsZzZ9X4rzcAGmeQTRYIn+LE6I85oP7hzfsdwV+f4kI+4b2jc6JNt
uhfeXRWqsjaL+L/GXLRCZ/PEOp/Gf+0qa5ENkAXsVTT8eHW5om/iUbJJF7PXvSfE
2cvmX4TYzgJz8e4UhTF2RxP5JSUSqYM2MFMxpznCTMTSY+gOniSghg/akrTFXYcx
/vw3OuyyOmhcgWp0n6BHFOTDNoLHNHnhRA9b9+Tm8Kq8aiPGXW+rORibGCKwCkxe
hlkCAwEAAaNgMF4wHwYDVR0jBBgwFoAU2HM0Q7JTLVjj+di0YU/FS9TfsXcwHQYD
VR0OBBYEFB1iXnF8w2l4s2IdDaKQQfEbzKzwMAwGA1UdEwEB/wQCMAAwDgYDVR0P
AQH/BAQDAgeAMA0GCSqGSIb3DQEBCwUAA4IBAQBzdQtPhl6YZoyUVwy2eh93E7O4
q22pQ3SUv4PYbkQNwFk1HDv68DSfNfWClzhjfBlsUaiQoWtJ6ZFHtwkQ2txe7xPv
1TFrr1yDL+4HkrxXH2W7z1W9l8XFzBo2q7kHBsy61mGlr3cvffZv9QogYjvfYcAT
8RCsTzKQ1GdiIgw/1h6cGhVCUGRAx3PwS56G4vNc7oE8ExD+iypMOT/+NJOHZB4K
nIDqF82DBxgk28RNemLn1LsRcyEsclzp5njdwyaqh+LbswXbnqbR2EavzP5XAsJk
s7H/8/ughG3JlvCNNNfzgDkBThWr/IIlwdw5wqjQ8gMe5vOOGV10siz+yCv8
-----END CERTIFICATE-----
)EOF";

static const char AWS_CERT_PRIVATE[] PROGMEM = R"EOF(
-----BEGIN RSA PRIVATE KEY-----
MIIEpAIBAAKCAQEAm59Gx6S+SrPuRf1eJQWmGdDZuHUxw0FRiYzeBIpVqGsK/Psp
FA17LCZjnkh6NEGag/7GRKOZWW7euYNEpWyii2mPRAxg74yxnNn1fivNwAaZ5BNF
gif4sTojzmg/uHN+x3BX5/iQj7hvaNzok226F95dFaqyNov4v8ZctEJn88Q6n8Z/
7SprkQ2QBexVNPx4dbmib+JRskkXs9e9J8TZy+ZfhNjOAnPx7hSFMXZHE/klJRKp
gzYwUzGnOcJMxNJj6A6eJKCGD9qStMVdhzH+/Dc67LI6aFyBanSfoEcU5MM2gsc0
eeFED1v35ObwqrxqI8Zdb6s5GJsYIrAKTF6GWQIDAQABAoIBAQCXiuv2/8NapYrT
Vx51eOG94/YQPPd/hzzqcGXHEBrHza0+mynuYA5g+OUrPrLZ7kSUYuYZ8yIix+dV
ybFwUbCbh+i+QDupKl3POyRngCp04zi6s9WNIHV5x/8UQcpDSRzdA9Zmi1Pi0JyV
BgHphwGfuqb0sWun2Hgb8ANYeg4WfqwKv9djW3tULpYOGgij9FQZIbRLlvl+L7Nk
nM+mKiUDVTwapJXq+7XOMvG47+4/cAbxCKYSSoP+WiN7hN1UkGMzS75JhhI1Ozu6
fdRWEcFoWZMfVwMZsnMBrAtWRTOd6AS/1JPcClecAteqEaldiIvauYu0W/h408TF
diT2OAixAoGBAM6uQKQ+VE6dALEDDon7+Ypb94ivZz3UVQ2mxc+/TiPUxm+1YhUY
b4KHV/YvMavuyUs4pPbaRMfqwaMG+GX+HRM6zHD/+mrI81KQNNZqhxa6b0A0iog/
lylRlCw7ctDLJE1NtOG7XiWqP4XFJK/WGvoLexBZTUTwOeBh4QnjzJ6VAoGBAMDB
8+u9VHOfsCLL0HgcmF9c8/nXJ+PXsNfInJIHnFIJbGtYIarjYEOCcaSgYGRSG96G
OaBO6wVIvk98FByorCVqa/THV/JS7XTOC69NUylDrsAIolZGzTYpJH7CwKzQ2CmJ
sIgqMBnx6Vi5fgXFUdXPOdR6iRzLIIdH+acL2gu1AoGAFfPaN9fK+qKgfSy28Z8K
7VBBZYpD7AROmGmbXyqRsSqbfSgF5/m1dmbLyAFRRFLTnKLCmtmqLpXXxWorHiI0
kmNPbb0yqv73IVDh29hqytY4lNg/0qL2elQI6f3SfyzkiTV0vfk50cRYhOvgrrCO
IvrvMlhZ5bWiYBvlXDiO9lUCgYEAqrdcS+4L7LeCbN7sDGTvAo61uhG3T5CJHCOd
n6vD49hawJt2ff4RFNljwvUTSeZ6rlNcmEEs3yo0+vqeaV9tz1l4sXsXxhNyISAX
szOdso8yJvu+owDj4NMBco8TzDrPJ8K9qWsL3P6mtyDZn0zKFL4Krsezxd+VOJsj
XbIedH0CgYB9q2jreAcBw9z1gbuXgDgJiB3n2nZJaSZ3gX9/NN3SpAc5kUKRnTMo
XggNZ9Xhu/qZuMoZmgjnxM/PNQt88v/szlHJ6Hy8ScH5K/74iO9pCgwaiZ2OwuCL
fwMyN834AOfJeFX+uOrfPDh7RL9HNZBd6zXkRAHLpIFMLuB/Y9qd0g==
-----END RSA PRIVATE KEY-----
)EOF";

bool sendCmd(const char* cmd, unsigned long timeout_ms = 2000) {
  Serial.print("[AT]> "); Serial.println(cmd);
  lteSerial.print(cmd);
  lteSerial.print("\r\n");

  unsigned long start = millis();
  String line;
  while (millis() - start < timeout_ms) {
    if (lteSerial.available()) {
      line = lteSerial.readStringUntil('\n');
      line.trim();
      if (line.length() == 0) continue;
      Serial.print("<Rsp> "); Serial.println(line);
      if (line == "OK") return true;
      if (line == "ERROR") return false;
    }
  }
  delay(20);
  Serial.println("<Err> sendCmd timeout");
  return false;
}

bool sendCmdWithPrompt(const char* cmd,
                       const char* payload, size_t payloadLen,
                       unsigned long promptTimeout_ms = 5000,
                       unsigned long finalTimeout_ms = 5000) {
  Serial.print("[AT]> "); Serial.println(cmd);
  lteSerial.print(cmd);
  lteSerial.print("\r\n");

  unsigned long start = millis();
  bool gotPrompt = false;
  while (millis() - start < promptTimeout_ms) {
    if (lteSerial.available()) {
      int c = lteSerial.read();
      Serial.write(c);
      if (c == '>') {
        gotPrompt = true;
        break;
      }
    }
  }
  if (!gotPrompt) {
    Serial.println("\n<Err> No '>' prompt received");
    return false;
  }

  Serial.println("\n[Info] Sending payload data...");
  size_t sent = 0;
  while (sent < payloadLen) {
    size_t chunk = min((size_t)256, payloadLen - sent);
    lteSerial.write((const uint8_t*)(payload + sent), chunk);
    sent += chunk;
    delay(10);
  }

  start = millis();
  String line;
  while (millis() - start < finalTimeout_ms) {
    if (lteSerial.available()) {
      line = lteSerial.readStringUntil('\n');
      line.trim();
      if (line.length() == 0) continue;
      Serial.print("<Rsp> "); Serial.println(line);
      if (line == "OK") return true;
      if (line == "ERROR") return false;
    }
  }
  Serial.println("<Err> sendCmdWithPrompt final timeout");
  return false;
}

bool storePemFile(const char* filename, const char* pem) {
  String delCmd = String("AT+UDELFILE=\"") + filename + "\"";
  sendCmd(delCmd.c_str(), 2000);

  size_t len = strlen(pem);
  String cmd = String("AT+UDWNFILE=\"") + filename + String("\",") + String(len);
  return sendCmdWithPrompt(cmd.c_str(), pem, len);
}

void setup() {
  Serial.begin(115200);
  lteSerial.begin(115200, SERIAL_8N1, RX_PIN, TX_PIN);
  delay(200);

  pinMode(PWR_ON_PIN, OUTPUT);
  digitalWrite(PWR_ON_PIN, LOW); // power pulse
  delay(2100);
  pinMode(PWR_ON_PIN, INPUT);
  delay(200);

  if (!sendCmd("AT")) {
    Serial.println("[Err] Module did not respond to AT");
  }

  if (!storePemFile("aws_ca.pem", AWS_CERT_CA)) {
    Serial.println("[Err] Failed to store aws_ca.pem");
  }
  if (!storePemFile("aws_client.crt", AWS_CERT_CLIENT)) {
    Serial.println("[Err] Failed to store aws_client.crt");
  }
  if (!storePemFile("aws_priv.key", AWS_CERT_PRIVATE)) {
    Serial.println("[Err] Failed to store aws_priv.key");
  }

  sendCmd("AT+ULSTFILE=2,\"aws_client.crt\""); // list file
  sendCmd("AT+ULSTFILE=2,\"aws_priv.key\"");   // list file

  sendCmd("AT+USECMNG=1,0,\"AWS_CA\",\"aws_ca.pem\"");       // root CA
  sendCmd("AT+USECMNG=1,1,\"AWS_Client\",\"aws_client.crt\""); // client cert
  sendCmd("AT+USECMNG=1,2,\"Client_Key\",\"aws_priv.key\"");   // client key

  sendCmd("AT+USECPRF=0,0,1");  // validation
  sendCmd("AT+USECPRF=0,2,0");  // cipher auto
  sendCmd("AT+USECPRF=0,3,\"AWS_CA\"");      // CA ref
  sendCmd("AT+USECPRF=0,5,\"AWS_Client\"");  // cert ref
  sendCmd("AT+USECPRF=0,6,\"Client_Key\"");  // key ref
  sendCmd("AT+USECPRF=0,10,\"ai6di095wqgo-ats.iot.us-east-1.amazonaws.com\""); // SNI

}

void loop() {}