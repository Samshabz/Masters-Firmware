
#include <Arduino.h>
#include <Wire.h>
#include <SPI.h>
#include <SD.h>
#include <OneWire.h>
#include <DallasTemperature.h>
#include <ArduinoJson.h>
#include <Adafruit_Sensor.h>
#include <Adafruit_ADXL343.h>
#include <time.h>
#include <stdlib.h>

#ifndef THINGNAME
#define THINGNAME "ESP32S2"
#endif

static constexpr const char* DEVICE_ID = "BMW_XIII";
static constexpr const char* MQTT_HOST = "ai6di095wqgo-ats.iot.us-east-1.amazonaws.com";
static constexpr const uint16_t MQTT_PORT_TLS = 8883;
static constexpr const char* MQTT_PUB_TOPIC = "esp32s2/pub";

struct PendingSend;

#define PIN_LTE_TX            1
#define PIN_LTE_RX            2

#define PIN_LTE_PWR          14

#ifndef LENA_GPIO_FIX_STATUS
#define LENA_GPIO_FIX_STATUS  16
#endif
#ifndef LENA_GPIO_NET_STATUS
#define LENA_GPIO_NET_STATUS  23
#endif

#define PIN_VOLT_DRDY         3
#define PIN_I2C0_SDA          4
#define PIN_I2C0_SCL          5

#define PIN_VOLT_CS           7
#define PIN_ADS_DRDY          8
#define PIN_ADS_CS            9

#define PIN_ADC_MISO         11
#define PIN_ADC_SCK          12
#define PIN_ADC_MOSI         13
#define PIN_CLKIN            10

#define PIN_CAN1_CS          21
#define PIN_CAN2_CS          37
#define PIN_CAN2_INT         39
#define PIN_CAN1_INT         40

#define PIN_TEMP_ONEWIRE     33

#define PIN_SD_MISO          34
#define PIN_SD_SCK           35
#define PIN_SD_MOSI          36
#define PIN_SD_CS            38

#define PIN_CAN_SCK          PIN_SD_SCK
#define PIN_CAN_MISO         PIN_SD_MISO
#define PIN_CAN_MOSI         PIN_SD_MOSI

const float HV_DIV_RATIO = 800.0f;

const float AMC_GAIN = 1.0f;

const float ADS_REF_V = 2.048f;

const float ADS_LSB_V = ADS_REF_V / 8388607.0f;

const float CURRENT_A_PER_V = 500.0f;

const unsigned long ADS_READ_INTERVAL_MS = 22;

static constexpr int32_t FLUX_CURR_CAN_ID = -1;

static constexpr uint8_t FLUX_CURR_BYTE_OFFSET = 1;

enum CurrentSensorMode {
  MODE_AUTO,
  MODE_ADS_ONLY,
  MODE_FLUX_ONLY,
  MODE_BOTH
};

struct Aggregator {

  double sumVoltage = 0.0;
  unsigned long countVoltage = 0;

  double sumCurrentAds = 0.0;
  unsigned long countCurrentAds = 0;

  double sumCurrentFlux = 0.0;
  unsigned long countCurrentFlux = 0;

  double sumAccelX = 0.0;
  double sumAccelY = 0.0;
  double sumAccelZ = 0.0;
  unsigned long countAccel = 0;

  double sumTemp = 0.0;
  unsigned long countTemp = 0;

  unsigned long vehCanCount = 0;
  unsigned long fluxCanCount = 0;

  bool gnssValid = false;
  double gnssLat = 0.0;
  double gnssLon = 0.0;
  double gnssAlt = 0.0;
  double gnssSpeed = NAN;
  double gnssHeading = NAN;
  float gnssHdop = 0.0f;
  int gnssSats = 0;

  void reset() {
    sumVoltage = 0.0; countVoltage = 0;
    sumCurrentAds = 0.0; countCurrentAds = 0;
    sumCurrentFlux = 0.0; countCurrentFlux = 0;
    sumAccelX = sumAccelY = sumAccelZ = 0.0; countAccel = 0;
    sumTemp = 0.0; countTemp = 0;
    vehCanCount = 0; fluxCanCount = 0;
  }
};

struct Sample {
  uint64_t epochMs;
  bool timeIsGnss;

  float hvVoltageV;
  float packCurrentA;
  CurrentSensorMode currentSrc;

  float accelX;
  float accelY;
  float accelZ;

  float tempC;

  bool gnssValid;
  double lat;
  double lon;
  double alt;
  double speed;
  double heading;
  float hdop;
  int sats;

  unsigned long vehCanCount;
  unsigned long fluxCanCount;
};

Adafruit_ADXL343 accel = Adafruit_ADXL343(12345, &Wire);

OneWire oneWire(PIN_TEMP_ONEWIRE);
DallasTemperature tempSensor(&oneWire);

SPIClass hspi(HSPI);
bool sdReady = false;
bool debugTrace = true;
static bool loopTimingVerbose = true;

static bool timeVerbose = false;

static const int32_t DISPLAY_TZ_OFFSET_SEC = 2 * 3600;
static void formatTzOffset(char* out, size_t n) {
  int32_t off = DISPLAY_TZ_OFFSET_SEC;
  char sign = '+';
  if (off < 0) { sign = '-'; off = -off; }
  int hh = off / 3600;
  int mm = (off % 3600) / 60;
  snprintf(out, n, "%c%02d:%02d", sign, hh, mm);
}

static bool flushVerbose = false;
static bool flushDebug = false;

static uint64_t lastProcessedSampleEpochMs = 0;
static uint64_t lastSummaryPrintedEpochMs = 0;
static unsigned long lastMainAttemptMs = 0;
static unsigned long lastMainSuccessMs = 0;
static unsigned long lastMainDurationMs = 0;
static bool          lastMainAttemptValid = false;
static bool          lastMainSuccessFlag = false;
static bool          flushRanRecent = false;
static size_t        flushSentRecent = 0;
static size_t        flushPendingRecent = 0;
static size_t        flushFailedRecent = 0;
static unsigned long flushTimeRecent = 0;
static size_t        backlogEnqueueSinceSummary = 0;

static uint8_t       flushLastBacklogOutcome = 0;

Aggregator aggregator;

static constexpr float MIN_HEADING_SPEED_MS = 0.3f;

static unsigned long nextGnssQueryMs = 0;
static unsigned long backlogFlushDueMs = 0;
static unsigned long rtcCheckDueMs = 0;
static uint8_t gnssMissRmc = 0;
static uint8_t gnssMissGga = 0;
static uint8_t gnssMissGsa = 0;
static unsigned long gnssLastFreshMs = 0;
static unsigned long gnssLastStaleWarnMs = 0;

CurrentSensorMode currentMode = MODE_ADS_ONLY;

unsigned long lastSampleMs = 0;
unsigned long lastRtcSyncMs = 0;
const unsigned long SAMPLE_INTERVAL_MS = 1000;
const unsigned long RTC_RESYNC_INTERVAL_MS = 15UL * 60UL * 1000UL; // 15 mins

bool gnssFixAvailable = false;
static uint32_t loopIteration = 0;
static bool sampleTickPending = false;
static unsigned long lastSampleTriggerMs = 0;
static const unsigned long GNSS_QUERY_INTERVAL_MS = 100;
static uint64_t pendingSampleEpochMs = 0;
static unsigned long sampleReadyAtMs = 0;
static const unsigned long SAMPLE_READY_DELAY_MS = 500; // wait half a second post-rollover before building the sample

HardwareSerial lteSerial(1);

static volatile bool gnssTimeAvailable = false;
static volatile uint64_t gnssEpochMs = 0;

static volatile uint8_t gnssFixDim = 0;
static bool lastRmcFixValid = false;

static unsigned long gnssTimeAnchorMillis = 0;

static volatile uint32_t lastFluxRaw24 = 0;

static bool lastSdOk = false;
static bool lastMqttOk = false;
static size_t lastPayloadLen = 0;

static bool mqttPending = false;
static unsigned long mqttAttemptMs = 0;
static unsigned long mqttLastSuccessMs = 0;
static uint8_t      mqttTimeoutCount = 0;
static unsigned long lastMqttConnectAttemptMs = 0;
static volatile bool lteSerialRxPending = false;

#if defined(ESP32)
static void IRAM_ATTR onLteSerialRx() { lteSerialRxPending = true; }
#else
static void onLteSerialRx() { lteSerialRxPending = true; }
#endif

enum { PENDING_NONE=0, PENDING_MAIN=1, PENDING_BACKLOG=2 };

struct PendingSend;
struct PendingSend {
  uint8_t kind;
  unsigned long enqMs;
  unsigned long timeoutMs;
  bool active;
  char path[40];
  String line;
  uint64_t sampleEpochMs;
  Sample sample;
  bool haveSample;
};
static const size_t MAX_PENDING_SENDS = 64;
static PendingSend pendingQ[MAX_PENDING_SENDS];
static size_t pendingQHead = 0, pendingQTail = 0, pendingQCount = 0;
static size_t pendingMainCount = 0;
static const unsigned long PENDING_MAIN_TIMEOUT_MS = 60000;

static bool pendingQIsFull(){ return pendingQCount >= MAX_PENDING_SENDS; }
static bool pendingQIsEmpty(){ return pendingQCount == 0; }
static PendingSend* pendingQFront(){ return pendingQIsEmpty()?nullptr:&pendingQ[pendingQHead]; }
static void pendingQPop(){
  if (pendingQIsEmpty()) return;
  PendingSend &s = pendingQ[pendingQHead];
  if (s.kind == PENDING_MAIN && pendingMainCount > 0) {
    pendingMainCount--;
  }
  s.active = false;
  s.line = "";
  s.sampleEpochMs = 0;
  s.haveSample = false;
  s.sample = Sample();
  pendingQHead = (pendingQHead + 1) % MAX_PENDING_SENDS;
  pendingQCount--;
  mqttPending = (pendingMainCount > 0);
}
static bool pendingQPushMain(unsigned long enqMs, unsigned long timeoutMs, const String &fallbackHex, const Sample &sample){
  if (pendingQIsFull()) return false;
  PendingSend &s = pendingQ[pendingQTail];
  s.kind = PENDING_MAIN;
  s.enqMs = enqMs;
  s.timeoutMs = timeoutMs;
  s.active = true;
  s.path[0] = '\0';
  s.line = fallbackHex;
  s.sampleEpochMs = sample.epochMs;
  s.sample = sample;
  s.haveSample = true;
  pendingQTail = (pendingQTail + 1) % MAX_PENDING_SENDS;
  pendingQCount++;
  pendingMainCount++;
  mqttPending = true;
  return true;
}
static bool pendingQPushBacklog(const char* path, const String &line, unsigned long enqMs, unsigned long timeoutMs){
  if (pendingQIsFull()) return false;
  PendingSend &s = pendingQ[pendingQTail];
  s.kind = PENDING_BACKLOG;
  s.enqMs = enqMs;
  s.timeoutMs = timeoutMs;
  s.active = true;
  strncpy(s.path, path ? path : "", sizeof(s.path));
  s.path[sizeof(s.path)-1] = '\0';
  s.line = line;
  s.sampleEpochMs = 0;
  s.haveSample = false;
  pendingQTail = (pendingQTail + 1) % MAX_PENDING_SENDS;
  pendingQCount++;
  return true;
}

struct BacklogRemoval { bool active; char path[40]; String line; };
static BacklogRemoval backlogDone[MAX_PENDING_SENDS];
static void recordBacklogRemoval(const char* path, const String &line){
  for (size_t i=0;i<MAX_PENDING_SENDS;i++){ if (!backlogDone[i].active){ backlogDone[i].active=true; strncpy(backlogDone[i].path, path?path:"", sizeof(backlogDone[i].path)); backlogDone[i].path[sizeof(backlogDone[i].path)-1]='\0'; backlogDone[i].line=line; break; } }
}

static bool backlogAppend(const String &hexPayload, uint64_t epochMs);
void storeSample(const Sample &s, int lteOk);

static void servicePendingTimeouts(){

  PendingSend *f = pendingQFront();
  if (!f) return;
  if (f->timeoutMs == 0) return;
  unsigned long age = millis() - f->enqMs;
  if (age < f->timeoutMs) return;

  if (f->kind == PENDING_MAIN) {
    lastMqttOk = false;
    if (f->haveSample) {
      storeSample(f->sample, 0);
      f->haveSample = false;
    }
    if (f->line.length()) {
      backlogAppend(f->line, f->sampleEpochMs);
    }
    if (mqttTimeoutCount < 250) mqttTimeoutCount++;
  }

  pendingQPop();
}

static bool adsPresent = false;

static volatile uint8_t amcDiscard = 0;

TwoWire &rtcI2C = Wire;

static bool lteDetected = false;
static bool lteMqttReady = false;

static uint8_t ltePowerPulsesUsed = 0;
static bool lteRuntimePulseDone = false;

static bool rtcFirstSynced = false;

static bool lteEverMqtt = false;

static const size_t BACKLOG_FLUSH_MAX_RECORDS = 9999999;
static const unsigned long BACKLOG_FLUSH_BUDGET_MS = 200;
static const unsigned long SYNC_CHECK_INTERVAL_MS = 1000;
static unsigned long nextSyncCheckMs = 0;
static unsigned long lastCsqPollMs = 0;
static int lastCsqValue = 99;
static bool lastCsqOk = false;
static uint32_t lastCsqPollLoop = 0xFFFFFFFFu;
static const int CSQ_MIN_GOOD = 3;
static const unsigned long MQTT_RECONNECT_GRACE_MS = 50000;
static const unsigned long MQTT_CONNECT_MIN_INTERVAL_MS = 3000;

static String sendCmd(const char* cmd, unsigned long timeoutMs, int expectedLines, int print_resp);

static int parseCsqValue(const String &resp) {
  int idx = resp.indexOf("+CSQ:");
  if (idx < 0) return -1;
  int comma = resp.indexOf(',', idx);
  if (comma <= idx + 5) return -1;
  int val = resp.substring(idx + 5, comma).toInt();
  if (val < 0 || val > 99) return -1;
  return val;
}

static int lteGetCsqValue(bool forcePoll) {
  if (!lteDetected) return 99;
  unsigned long now = millis();
  bool alreadyPolledThisLoop = (lastCsqPollLoop == loopIteration);
  bool stale = (lastCsqPollMs == 0) || (now - lastCsqPollMs) >= SAMPLE_INTERVAL_MS;

  if (!forcePoll) {
    if (alreadyPolledThisLoop || (!stale && lastCsqOk)) {

      return lastCsqValue;
    }
  } else if (alreadyPolledThisLoop && !stale) {

    return lastCsqValue;
  }

  String resp = sendCmd("AT+CSQ", 300, 1, 0);
  int val = parseCsqValue(resp);
  lastCsqValue = (val >= 0) ? val : 99;
  lastCsqOk = (lastCsqValue >= CSQ_MIN_GOOD) && (lastCsqValue != 99);
  lastCsqPollMs = millis();
  lastCsqPollLoop = loopIteration;
  return lastCsqValue;
}

static bool lteSignalSufficient(bool forcePoll, const char *gateReason) {
  if (!lteDetected) return false;
  if (forcePoll || lastCsqPollMs == 0 || (!lastCsqOk && lastCsqPollLoop != loopIteration)) {
    (void)lteGetCsqValue(true);
  } else {
    (void)lteGetCsqValue(false);
  }
  if (!lastCsqOk && gateReason && debugTrace) {
    Serial.printf("[LTE] skip %s: CSQ=%d\n", gateReason, lastCsqValue);
  }

  return lastCsqOk;
}

void setupSensors();
void setupStorage();

static void pollGnssTime();
static void testCSQ();
static void ResyncRTC();
static void flushBacklog();
static void processSample();
static void processSampleIfReady();
static void printResetInfo();
static void initAppState();
static void printSampleOutput(const Sample &s, bool vOk, bool accOk, bool tempOk, bool gnssOk);
static void printLteSummaryIfNeeded();
static bool backlogTodayExistsQuick(uint64_t epochMs);
void readSensors();
bool sampleReady();
Sample buildSample();
bool validateSample(const Sample &s);
void storeSample(const Sample &s, int lteOk);
void publishSample(const Sample &s);
bool syncNeeded();
void syncWithCloud();
void pumpModemUrc(unsigned long budgetMs = 50);

void initVoltageSensor();
void initCurrentSensorADS();
void initFluxgateCanBus();
void initVehicleCanBus();
void initAccelerometer();
void initTempSensor();
void initRTC();
void printRtcDiagnostics();
void initLteGnss();
static void holdAllChipSelectsHigh();
static inline void adcBusIdle();

void readVoltage();
void readCurrentADS();
void readFluxgateCAN();
void readVehicleCAN();
void readAccelerometer();
void readTemperature();
void readGnss();

uint64_t getEpochMs(bool &isGnss);
void handleRtcResync(unsigned long currentMs);

float convertAdsCountsToVolts(int32_t code);
float convertFluxRawToCurrent(uint32_t raw);
float convertAdsCountsToCurrent(int32_t code);

static String sendCmd(const char* cmd, unsigned long timeoutMs = 20, int expectedLines = 2, int print_resp=0);
static bool   initLTE();
static bool   ensureLTEConnected();
static bool   publishHexPayload(const String &hexPayload, const String &fallbackHex, const Sample &sample);

static bool   publishHexPayloadBacklogEnqueue(const char* path, const String &hexPayload);
static void   gnssEnableLastNmea();

static String gnssFetchLast(const char* cmd, const char* tag, uint8_t &missCounter,
                            unsigned long timeoutMs, int expectedLines);
static bool   backlogExists();
static bool   findOldestBacklogFile(char* outPath, size_t outLen);
static size_t flushBacklogMulti(size_t maxRecords, unsigned long budgetMs);
static bool   backlogAppend(const String &hexPayload, uint64_t epochMs);

static void   lenaGpioInit();
static void   lenaGpioSetFix(bool on);
static void   lenaGpioSetMqtt(bool on);

bool rtcStartOscillator();
bool rtcSetDateTime(uint16_t year, uint8_t month, uint8_t day, uint8_t weekday,
                    uint8_t hour, uint8_t minute, uint8_t second);
bool rtcGetDateTime(uint16_t &year, uint8_t &month, uint8_t &day, uint8_t &weekday,
                    uint8_t &hour, uint8_t &minute, uint8_t &second);
uint32_t rtcGetEpoch();
void rtcSyncFromEpoch(time_t epoch);

void setup() {

  Serial.begin(115200);
  delay(700);

  Serial.println("BOOT: starting Telematics V2...");
  Serial.flush();
  delay(10);
  printResetInfo();
  setupSensors();  // all sensors
  setupStorage(); // sd
  initAppState(); //reset vars
}

void loop() {

  loopIteration++;
  int time_0 = millis();
  if (lteSerialRxPending) {

    pumpModemUrc(5); // check if we have received any urc data e.g., LTE pub success
    lteSerialRxPending = false;
  }
  int time_1 = millis();

  pollGnssTime(); // check gnss or rtc time
  int  time_2 = millis();

  processSample(); // if 500ms past rollover aggregate; pub therafter
  int  time_3 = millis();

  servicePendingTimeouts();  // correspond the latest urc status to the oldest unresolved lte publish: store on sd with lte_flag based on fail/succ and also append to backlog if failed
  int  time_4 = millis();

  readSensors(); // read all sensors - excludes gnss 
   int time_5 = millis();

  testCSQ(); // test whether csq is 
  int  time_6 = millis();

  ResyncRTC(); // resync if on startup or 15 minutes
  int  time_7 = millis();

  flushBacklog(); // if time and quantity limit not exceeded, flush. 

}

static void printResetInfo() {
#if defined(ESP32)
  #include <esp_system.h>
  esp_reset_reason_t rr = esp_reset_reason();
  Serial.printf("[DBG] reset_reason=%d freeHeap=%u\n", (int)rr, (unsigned)ESP.getFreeHeap());
#endif
}

static void resetLoopSchedule(unsigned long baseMs) {
  nextGnssQueryMs    = millis() + GNSS_QUERY_INTERVAL_MS;
  backlogFlushDueMs    = baseMs + 850;
  rtcCheckDueMs        = baseMs + 900;
}

static void initAppState() {
  aggregator.reset();
  lastSampleMs = millis();
  lastRtcSyncMs = millis();
  lastSampleTriggerMs = lastSampleMs;
  sampleTickPending = false;
  pendingSampleEpochMs = 0;
  nextGnssQueryMs = 0;
  lastRmcFixValid = false;
  resetLoopSchedule(lastSampleMs);
}

static bool parseGnssRmcSentence(const String &nmea, unsigned long nowMs, bool &secondAdvanced) {
  secondAdvanced = false;
  if (!nmea.startsWith("$GPRMC") && !nmea.startsWith("$GNRMC")) {
    return false;
  }

  String f[16]; int nf = 0; int start = 0;
  for (int i = 0; i <= nmea.length() && nf < 16; ++i) {
    if (i == nmea.length() || nmea.charAt(i) == ',') {
      f[nf++] = nmea.substring(start, i);
      start = i + 1;
    }
  }
  if (nf < 10) return false;

  auto dmToDeg = [](const String &dm)->double{
    if (dm.length() < 3) return NAN;
    double v = dm.toFloat();
    int dd = int(v / 100.0);
    double mm = v - dd * 100.0;
    return double(dd) + mm / 60.0;
  };

  bool fixValid = (f[2] == "A");
  lastRmcFixValid = fixValid;

  if (fixValid && f[3].length() && f[5].length()) {
    double lat = dmToDeg(f[3]); if (f[4] == "S") lat = -lat;
    double lon = dmToDeg(f[5]); if (f[6] == "W") lon = -lon;
    if (isfinite(lat) && isfinite(lon)) {
      aggregator.gnssLat = lat;
      aggregator.gnssLon = lon;
    }
  }

  double spd = NAN;
  if (f[7].length()) spd = f[7].toFloat() * 0.514444;
  double newHeading = f[8].length() ? f[8].toFloat() : NAN;
  aggregator.gnssSpeed = (fixValid && isfinite(spd)) ? spd : NAN;
  aggregator.gnssHeading = (fixValid && isfinite(spd) && spd > MIN_HEADING_SPEED_MS) ? newHeading : NAN;

  if (fixValid && f[1].length() >= 6 && f[9].length() == 6) {
    int hh = f[1].substring(0,2).toInt();
    int mm = f[1].substring(2,4).toInt();
    int ss = f[1].substring(4,6).toInt();
    int day = f[9].substring(0,2).toInt();
    int mon = f[9].substring(2,4).toInt();
    int yy  = f[9].substring(4,6).toInt();
    int year = 2000 + yy;
    struct tm tmv{};
    tmv.tm_year = year - 1900;
    tmv.tm_mon = mon - 1;
    tmv.tm_mday = day;
    tmv.tm_hour = hh;
    tmv.tm_min = mm;
    tmv.tm_sec = ss;
    auto tmToEpochUTC = [](const struct tm& t)->time_t{
      int Y = t.tm_year + 1900; int M = t.tm_mon + 1; int D = t.tm_mday;
      int hh = t.tm_hour; int mm = t.tm_min; int ss = t.tm_sec;
      int y = Y - (M <= 2);
      int era = (y >= 0 ? y : y - 399) / 400;
      unsigned yoe = (unsigned)(y - era * 400);
      unsigned doy = (unsigned)((153 * (M + (M > 2 ? -3 : 9)) + 2)/5 + D - 1);
      unsigned doe = yoe*365 + yoe/4 - yoe/100 + doy;
      int64_t days = (int64_t)era*146097 + (int64_t)doe - 719468;
      return (time_t)(days*86400LL + hh*3600 + mm*60 + ss);
    };
    time_t epoch = tmToEpochUTC(tmv);
    if (epoch > 0) {
      uint64_t previous = gnssEpochMs;
      uint64_t epochMs = (uint64_t)epoch * 1000ULL;
      gnssEpochMs = epochMs;
      gnssTimeAnchorMillis = nowMs;
      gnssTimeAvailable = true;
      uint64_t prevSecond = previous ? previous / 1000ULL : 0;
      uint64_t newSecond = epochMs / 1000ULL;
      secondAdvanced = (previous == 0) || (newSecond != prevSecond);
      if (!rtcFirstSynced) {
        rtcSyncFromEpoch((time_t)(epochMs / 1000ULL));
        rtcFirstSynced = true;
        if (debugTrace) Serial.println("[TIME] RTC synced from GNSS (first sync)");
      }
    }
  }

  aggregator.gnssValid = fixValid && (gnssFixDim >= 3);
  gnssFixAvailable = aggregator.gnssValid;
  return true;
}

static void fetchGnssAuxData(unsigned long nowMs) {
  bool gotData = false;
  auto dmToDeg = [](const String &dm)->double{
    if (dm.length() < 3) return NAN;
    double v = dm.toFloat();
    int dd = int(v / 100.0);
    double mm = v - dd * 100.0;
    return double(dd) + mm / 60.0;
  };

  String r1 = gnssFetchLast("AT+UGGGA?", "+UGGGA:", gnssMissGga, 250, 1);
  int p = r1.indexOf("+UGGGA:");
  if (p >= 0) {
    int comma = r1.indexOf(',', p);
    int nl = r1.indexOf('\n', p);
    if (comma > 0) {
      String nmea = r1.substring(comma + 1, nl > comma ? nl : r1.length());
      nmea.trim();
      if (nmea.startsWith("$GPGGA") || nmea.startsWith("$GNGGA")) {
        String f[16]; int nf = 0; int start = 0;
        for (int i = 0; i <= nmea.length() && nf < 16; ++i) {
          if (i == nmea.length() || nmea.charAt(i) == ',') {
            f[nf++] = nmea.substring(start, i);
            start = i + 1;
          }
        }
        if (nf >= 10) {
          if (f[2].length() && f[4].length()) {
            double lat = dmToDeg(f[2]); if (f[3] == "S") lat = -lat;
            double lon = dmToDeg(f[4]); if (f[5] == "W") lon = -lon;
            if (isfinite(lat) && isfinite(lon)) {
              aggregator.gnssLat = lat;
              aggregator.gnssLon = lon;
            }
          }
          if (f[7].length()) aggregator.gnssSats = f[7].toInt();
          if (f[8].length()) aggregator.gnssHdop = f[8].toFloat();
          if (f[9].length()) aggregator.gnssAlt  = f[9].toFloat();
          gotData = true;
        }
      }
    }
  }

  String r2 = gnssFetchLast("AT+UGGSA?", "+UGGSA:", gnssMissGsa, 250, 1);
  int q = r2.indexOf("+UGGSA:");
  if (q >= 0) {
    int comma = r2.indexOf(',', q);
    int nl = r2.indexOf('\n', q);
    if (comma > 0) {
      String nmea = r2.substring(comma + 1, nl > comma ? nl : r2.length());
      nmea.trim();
      if (nmea.startsWith("$GPGSA") || nmea.startsWith("$GNGSA")) {
        String f[18]; int nf = 0; int start = 0;
        for (int i = 0; i <= nmea.length() && nf < 18; ++i) {
          if (i == nmea.length() || nmea.charAt(i) == ',') {
            f[nf++] = nmea.substring(start, i);
            start = i + 1;
          }
        }
        if (nf >= 3 && f[2].length()) {
          int fixType = f[2].toInt();
          if (fixType >= 0 && fixType <= 3) {
            gnssFixDim = (uint8_t)fixType;
            gotData = true;
          }
        }
      }
    }
  }

  aggregator.gnssValid = lastRmcFixValid && (gnssFixDim >= 3);
  gnssFixAvailable = aggregator.gnssValid;

  if (gotData) {
    gnssLastFreshMs = nowMs;
  }
}

static void pollGnssTime() {
  unsigned long now = millis();

  static uint32_t lastRtcEpoch = 0;
  static uint64_t lastScheduledSampleEpochMs = 0;

  if (now < nextGnssQueryMs) return;
  nextGnssQueryMs = now + GNSS_QUERY_INTERVAL_MS;

  bool secondAdvanced = false;
  bool hadSentence = false;

  String resp = gnssFetchLast("AT+UGRMC?", "+UGRMC:", gnssMissRmc, 255, 1);

  int idx = resp.indexOf("+UGRMC:");
  if (idx >= 0) {
    int comma = resp.indexOf(',', idx);
    int nl = resp.indexOf('\n', idx);
    if (comma > 0) {
      String nmea = resp.substring(comma + 1, nl > comma ? nl : resp.length());
      nmea.trim();
      if (nmea.length()) {
        hadSentence = parseGnssRmcSentence(nmea, now, secondAdvanced);
      }
    }
  }

  if (hadSentence) {
    gnssLastFreshMs = now;
    gnssLastStaleWarnMs = now;
  }

  uint32_t rtcEpoch = 0;
  bool rtcEpochLoaded = false;
  auto ensureRtcEpoch = [&]()->uint32_t {
    if (!rtcEpochLoaded) {
      rtcEpoch = rtcGetEpoch();
      rtcEpochLoaded = true;
    }
    return rtcEpoch;
  };
  auto tryScheduleSample = [&](uint64_t epochMs)->bool {
    if (epochMs == 0) return false;
    if (lastScheduledSampleEpochMs == epochMs) return false;
    sampleTickPending = true;
    pendingSampleEpochMs = epochMs;
    lastSampleTriggerMs = now;
    sampleReadyAtMs = lastSampleTriggerMs + SAMPLE_READY_DELAY_MS;
    lastScheduledSampleEpochMs = epochMs;
    return true;
  };

  if (secondAdvanced) {
        if (gnssTimeAvailable) {
      uint32_t rtcEpoch = rtcGetEpoch();
      if (rtcEpoch > 0) {
        int64_t gnssSec = (int64_t)(gnssEpochMs / 1000ULL);
        int64_t diffSec = gnssSec - (int64_t)rtcEpoch;
        Serial.printf("[TIME] GNSS-RTC delta: %lld s\n", (long long)diffSec);
      }
    }
    fetchGnssAuxData(now);

    if (!aggregator.gnssValid || !lastRmcFixValid) {
      uint32_t epoch = ensureRtcEpoch();
      if (epoch > 0) {
        if (tryScheduleSample((uint64_t)epoch * 1000ULL)) { // check if its 500ms post-rollover
          aggregator.gnssValid = false;
          gnssFixAvailable = false;
          if (debugTrace) {
            Serial.println("[TIME] No GNSS fix — using RTC epoch");
          }
        }
        lastRtcEpoch = epoch;
      }
    } else {
      uint64_t sampleEpoch = (gnssEpochMs >= SAMPLE_INTERVAL_MS)
        ? (gnssEpochMs - SAMPLE_INTERVAL_MS)
        : gnssEpochMs;
      (void)tryScheduleSample(sampleEpoch);
      uint32_t epoch = ensureRtcEpoch();
      if (epoch > 0) {
        lastRtcEpoch = epoch;
      }
    }
  }

  bool requestedRtcFallback = false;
  if (hadSentence && !secondAdvanced) {

    uint32_t epoch = ensureRtcEpoch();
    if (epoch > 0 && epoch != lastRtcEpoch) {
      requestedRtcFallback = true;
    }
  }

  if (!hadSentence && gnssLastFreshMs && (now - gnssLastFreshMs) > 5000UL) {
    aggregator.gnssValid = false;
    gnssFixAvailable = false;
    if (debugTrace && (now - gnssLastStaleWarnMs) > 2000UL) {
      Serial.printf("[GNSSWARN] NMEA stale for %lums\n", (unsigned long)(now - gnssLastFreshMs));
      gnssLastStaleWarnMs = now;
    }
  }

  if (!hadSentence || requestedRtcFallback) {
    uint32_t epoch = ensureRtcEpoch();
    if (epoch > 0 && epoch != lastRtcEpoch) {

      bool scheduled = tryScheduleSample((uint64_t)epoch * 1000ULL);
      lastRtcEpoch = epoch;
      if (scheduled) {
        if (!hadSentence) {

          aggregator.gnssValid = false;
          gnssFixAvailable = false;
          if (debugTrace) {
            Serial.println("[TIME] GNSS missing — RTC second edge tick");
          }
        } else if (debugTrace && !aggregator.gnssValid) {
          Serial.println("[TIME] GNSS invalid — RTC assist tick");
        }
      }
    } else {

    }
  }

}

static void printSampleOutput(const Sample &s, bool vOk, bool accOk, bool tempOk, bool gnssOk) {
  auto SF = [](bool ok){ return ok ? 'S' : 'F'; };

  bool csqOk = lteSignalSufficient(false, nullptr);
  bool mqttOkDisplay = (lteMqttReady && csqOk) || ((millis() - mqttLastSuccessMs) < 1500);
  lenaGpioSetMqtt(mqttOkDisplay);

  char ts[32]; time_t tsec = (time_t)(s.epochMs / 1000ULL);
  tsec += DISPLAY_TZ_OFFSET_SEC;
  struct tm tmv; gmtime_r(&tsec, &tmv);
  snprintf(ts, sizeof(ts), "%04d-%02d-%02d %02d:%02d:%02d SAT",
           tmv.tm_year+1900, tmv.tm_mon+1, tmv.tm_mday, tmv.tm_hour, tmv.tm_min, tmv.tm_sec);
  auto valOrF = [](double v, const char* fmt){ if (isfinite(v)) { char buf[32]; snprintf(buf, sizeof(buf), fmt, v); Serial.print(buf); } else { Serial.print('F'); } };
  Serial.print("[OUTPUT] ");
  Serial.print(ts); Serial.print(' ');
  Serial.print("V=");   valOrF(vOk ? s.hvVoltageV : NAN, "%.3fV");
  Serial.print(" I=");   valOrF(isfinite(s.packCurrentA) ? s.packCurrentA : NAN, "%.3fA");
  Serial.print(" ax=");  valOrF(accOk ? s.accelX : NAN, "%.3f");
  Serial.print(" ay=");  valOrF(accOk ? s.accelY : NAN, "%.3f");
  Serial.print(" az=");  valOrF(accOk ? s.accelZ : NAN, "%.3f m/s^2");
  Serial.print(" temp=");valOrF(tempOk ? s.tempC : NAN, "%.2fC");
  Serial.printf(" MQTT=%c SD=%c\n", SF(mqttOkDisplay), SF(lastSdOk));

  Serial.print("[GNSS] ");
  Serial.print("valid="); Serial.print(gnssOk ? 1 : 0);
  Serial.print(" lat=");   valOrF(gnssOk ? s.lat : NAN, "%.6f");
  Serial.print(" lon=");   valOrF(gnssOk ? s.lon : NAN, "%.6f");
  Serial.print(" alt=");   valOrF(gnssOk ? s.alt : NAN, "%.1f");
  Serial.print(" speed="); valOrF(gnssOk ? s.speed : NAN, "%.3f");
  Serial.print(" heading=");valOrF(gnssOk ? s.heading : NAN, "%.3f");
  Serial.print(" hdop=");  valOrF(gnssOk ? s.hdop : NAN, "%.2f");
  Serial.print(" sats=");  if (gnssOk) Serial.print(s.sats); else Serial.print('F');
  Serial.println();
}

static void processSampleIfReady() {
  if (!sampleReady()) return;
  Sample s = buildSample();
  pendingSampleEpochMs = 0;
  bool vOk     = (aggregator.countVoltage     > 0);
  bool iAdsOk  = (aggregator.countCurrentAds  > 0);
  bool iFluxOk = (aggregator.fluxCanCount > 0);
  bool accOk   = (aggregator.countAccel       > 0);
  bool tempOk  = (aggregator.countTemp        > 0);
  bool gnssOk  = s.gnssValid;
  (void)iFluxOk;

  bool sensorsOk = vOk && (iAdsOk || iFluxOk) && accOk && tempOk && gnssOk;
  lenaGpioSetFix(sensorsOk);

  if (validateSample(s)) {
    publishSample(s);
    printSampleOutput(s, vOk, accOk, tempOk, gnssOk);
  }
  aggregator.reset();
  if (lastSampleTriggerMs == 0) {
    lastSampleTriggerMs = millis();
  }
  lastSampleMs = lastSampleTriggerMs;
  resetLoopSchedule(lastSampleMs);

  lastProcessedSampleEpochMs = s.epochMs;
}

static bool backlogTodayExistsQuick(uint64_t epochMs) {
  if (!sdReady) return false;
  uint16_t y; uint8_t mon, day;
  if (epochMs) {
    time_t t = (time_t)(epochMs / 1000ULL); struct tm tmv; gmtime_r(&t, &tmv);
    y = tmv.tm_year + 1900; mon = tmv.tm_mon + 1; day = tmv.tm_mday;
  } else {
    uint8_t wd, h, m, s;
    if (!rtcGetDateTime(y, mon, day, wd, h, m, s)) return false;
  }
  char blfn[32]; sprintf(blfn, "/backlog_%04d%02d%02d.txt", y, mon, day);
  return SD.exists(blfn);
}

static void printLteSummaryIfNeeded() {
  if (!debugTrace) return;
  if (lastProcessedSampleEpochMs == 0) return;
  if (lastSummaryPrintedEpochMs == lastProcessedSampleEpochMs) return;

  bool backlogExistsNow = backlogTodayExistsQuick(lastProcessedSampleEpochMs);
  char mainChar = lastMainSuccessFlag ? 'T' : 'F';
  unsigned long mainMs = lastMainSuccessFlag ? lastMainDurationMs : (mqttPending ? (millis() - lastMainAttemptMs) : 0);
  unsigned long flushMs = flushRanRecent ? flushTimeRecent : 0;
  size_t flushed = flushRanRecent ? flushSentRecent : 0;
  size_t pending = flushRanRecent ? flushPendingRecent : 0;
  size_t failed  = flushRanRecent ? flushFailedRecent : 0;
  // Serial.printf("[LTE] backlog=%d main=%c %lums flush=%uT/%uP/%uF %lums\n",
  //               backlogExistsNow ? 1 : 0, mainChar, mainMs,
  //               (unsigned)flushed, (unsigned)pending, (unsigned)failed, flushMs);
  lastSummaryPrintedEpochMs = lastProcessedSampleEpochMs;

  flushRanRecent = false;
  flushSentRecent = 0;
  flushPendingRecent = 0;
  flushFailedRecent = 0;
  flushTimeRecent = 0;
  backlogEnqueueSinceSummary = 0;
}

static void ResyncRTC() {
  unsigned long now = millis();
  if (rtcCheckDueMs == 0) {
    rtcCheckDueMs = lastSampleMs + 900;
  }
  if (now < rtcCheckDueMs) {
    return;
  }
  rtcCheckDueMs += SAMPLE_INTERVAL_MS;

  handleRtcResync(now);
}

static void flushBacklog() {
  unsigned long now = millis();
  if (backlogFlushDueMs == 0) {
    backlogFlushDueMs = lastSampleMs + 850;
    if (flushDebug && debugTrace) {
      Serial.printf("[FLUSHDBG] schedule first flush at %lu (now=%lu)\n", backlogFlushDueMs, now);
    }
  }
  if (now < backlogFlushDueMs) {
    if (flushDebug && debugTrace) {
      Serial.printf("[FLUSHDBG] skip: now<due (%lu<%lu)\n", now, backlogFlushDueMs);
    }
    return;
  }
  if ((now - lastSampleMs) > 750UL) {
    bool backlogLikely = backlogTodayExistsQuick(lastProcessedSampleEpochMs);
    if (pendingQIsEmpty() && !backlogLikely) {
      backlogFlushDueMs += SAMPLE_INTERVAL_MS;
      if (flushDebug && debugTrace) {
        Serial.printf("[FLUSHDBG] defer: inactive window (Δ=%lu, no backlog/pending)\n", now - lastSampleMs);
      }
      return;
    }
    if (flushDebug && debugTrace) {
      Serial.printf("[FLUSHDBG] continuing despite inactive window (Δ=%lu backlog=%d pending=%d)\n",
                    now - lastSampleMs, backlogLikely ? 1 : 0, pendingQIsEmpty() ? 0 : 1);
    }
  }
  backlogFlushDueMs += SAMPLE_INTERVAL_MS;
  if (flushDebug && debugTrace) {
    Serial.printf("[FLUSHDBG] running at %lu nextDue=%lu\n", now, backlogFlushDueMs);
  }

  servicePendingTimeouts();
  if (syncNeeded()) {
    if (flushDebug && debugTrace) {
      Serial.println("[FLUSHDBG] syncNeeded -> syncWithCloud");
    }
    syncWithCloud();
  } else if (flushDebug && debugTrace) {
    Serial.println("[FLUSHDBG] syncNeeded false — no flush");
  }
  if (lteSerialRxPending) {
    if (flushDebug && debugTrace) {
      Serial.println("[FLUSHDBG] pumpModemUrc due to pending RX");
    }
    pumpModemUrc(1);
    lteSerialRxPending = false;
  }
  printLteSummaryIfNeeded();
}

static void processSample() {
  processSampleIfReady();
}

void setupSensors() {

  holdAllChipSelectsHigh();

  hspi.begin(PIN_SD_SCK, PIN_SD_MISO, PIN_SD_MOSI, -1);

  SPI.begin(PIN_ADC_SCK, PIN_ADC_MISO, PIN_ADC_MOSI, -1);
  delay(20);

  Wire.begin(PIN_I2C0_SDA, PIN_I2C0_SCL);
  Wire.setTimeOut(100);

  Wire1.begin(PIN_I2C0_SDA, PIN_I2C0_SCL);

  initVoltageSensor();
  initCurrentSensorADS();

  initFluxgateCanBus();
  initVehicleCanBus();
  initAccelerometer();
  initTempSensor();
  initRTC();
  initLteGnss();

}

void setupStorage() {

  holdAllChipSelectsHigh();

  sdReady = SD.begin(PIN_SD_CS, hspi);
  if (sdReady) {

    uint64_t cap = SD.cardSize();
    uint8_t type = SD.cardType();
    Serial.printf("[SD] OK (CS=%d, SCK=%d, MISO=%d, MOSI=%d) type=%u size=%llu MB\n",
                  PIN_SD_CS, PIN_SD_SCK, PIN_SD_MISO, PIN_SD_MOSI,
                  (unsigned)type, (unsigned long long)(cap / (1024ULL*1024ULL)));

    File root = SD.open("/");
    if (!root) { Serial.println("[SD] Root open failed unexpectedly"); }
    else root.close();
  } else {
    Serial.println("[SD] Failed to initialise. Logging disabled.");
    Serial.printf("[SD] HSPI pins: CS=%d SCK=%d MISO=%d MOSI=%d\n", PIN_SD_CS, PIN_SD_SCK, PIN_SD_MISO, PIN_SD_MOSI);
    Serial.println("[SD] Tips: ensure CAN CS pins are HIGH, share HSPI cleanly, and try removing CAN init to isolate.");
    debugTrace = true;
  }
}

static void holdAllChipSelectsHigh() {

  pinMode(PIN_VOLT_CS, OUTPUT); digitalWrite(PIN_VOLT_CS, HIGH);
  pinMode(PIN_ADS_CS,  OUTPUT); digitalWrite(PIN_ADS_CS,  HIGH);

  pinMode(PIN_SD_CS,   OUTPUT); digitalWrite(PIN_SD_CS,   HIGH);
  pinMode(PIN_CAN1_CS, OUTPUT); digitalWrite(PIN_CAN1_CS, HIGH);
  pinMode(PIN_CAN2_CS, OUTPUT); digitalWrite(PIN_CAN2_CS, HIGH);
}

static inline void adcBusIdle() {
  digitalWrite(PIN_ADS_CS, HIGH);
  digitalWrite(PIN_VOLT_CS, HIGH);

  delayMicroseconds(1);
}

static void testCSQ() {
  if (!lteDetected) return;
  unsigned long now = millis();

  bool needPoll = false;
  if (!lastCsqOk) {
    needPoll = (lastCsqPollLoop != loopIteration);
  } else if (lastCsqPollMs == 0 || (now - lastCsqPollMs) >= SAMPLE_INTERVAL_MS) {
    needPoll = true;
  }

  if (needPoll) {
    int val = lteGetCsqValue(true);
    if (lastCsqOk && !lteMqttReady) {
      if (debugTrace) {
        Serial.printf("[LTE] CSQ=%d -> reconnect attempt\n", val);
      }
      (void)ensureLTEConnected();
    }
  } else {
    (void)lteGetCsqValue(false);
  }
}

static bool atVerbose = false;

static bool lteProbeAT(unsigned long timeoutMs = 200) {
  String probe = sendCmd("AT", timeoutMs, 2);
  return probe.length() > 0;
}

static void ltePowerPulse(bool bool_ON=1) {
  if (PIN_LTE_PWR >= 0 && bool_ON) {
    Serial.println("[MODEM] Power pulse (2.1s)…");
    Serial.flush();
    delay(200);
    pinMode(PIN_LTE_PWR, OUTPUT);
    digitalWrite(PIN_LTE_PWR, LOW);
    delay(2100);
    pinMode(PIN_LTE_PWR, INPUT);
    delay(50);
  }

  else if (!bool_ON){
    Serial.println("[MODEM] Power OFF (3.2s)…");
    Serial.flush();
    delay(200);
    pinMode(PIN_LTE_PWR, OUTPUT);
    digitalWrite(PIN_LTE_PWR, LOW);
    delay(3200);
    pinMode(PIN_LTE_PWR, INPUT);
    delay(450);

    Serial.println("[MODEM] Power ON (2.1s)…");
    Serial.flush();
    delay(200);
    pinMode(PIN_LTE_PWR, OUTPUT);
    digitalWrite(PIN_LTE_PWR, LOW);
    delay(3100);
    pinMode(PIN_LTE_PWR, INPUT);
    delay(50);

  }

}

static String sendCmd(const char* cmd, unsigned long timeoutMs, int expectedLines, int print_resp) {

  print_resp = 0;
  const unsigned long MIN_CMD_DELAY_MS = 60;
  static unsigned long lastSendTime = 0;
  unsigned long now = millis();
  if (now - lastSendTime < MIN_CMD_DELAY_MS) {
    delay(MIN_CMD_DELAY_MS - (now - lastSendTime));
  }

  {
    unsigned long quietSince = millis();
    while (millis() - quietSince < 15) {
      while (lteSerial.available()) { lteSerial.read(); quietSince = millis(); }
      delay(1);
    }
  }

  unsigned long opStart = millis();
  String resp = "";
  int count = 0;

  if (strcmp(cmd, "WAIT") == 0) {
    Serial.println("[LTE>>] WAIT");
    while (millis() - opStart < timeoutMs) {
      while (lteSerial.available()) {
        String line = lteSerial.readStringUntil('\n');
        line.trim();
        if (line.length()) {

          resp += line + '\n';
          count++;
          if (count >= expectedLines) break;
        }
      }
      if (count >= expectedLines) break;
      delay(1);
    }
    unsigned long elapsed = millis() - opStart;

    return resp;
  }

  lteSerial.print(cmd);
  lteSerial.print("\r\n");
  lastSendTime = millis();

  while (millis() - opStart < timeoutMs) {
    if (!lteSerial.available()) { delay(1); continue; }
    String line = lteSerial.readStringUntil('\n');
    line.trim();
    if (!line.length()) continue;

    bool isNmea = (line.startsWith("$GP") || line.startsWith("$GN"));

    resp += line + '\n';
    if (!isNmea) {
      count++;
      if (count >= expectedLines) break;
    }
  }

  unsigned long elapsed = millis() - opStart;

  if (count < expectedLines) Serial.printf("[LTEDBG] Timeout: got %d/%d lines\n", count, expectedLines);

  return resp;
}

void pumpModemUrc(unsigned long budgetMs) {
  static String lineBuf;
  unsigned long t0 = millis();

  while (lteSerial.available() && (millis() - t0) < budgetMs) {
    char c = (char)lteSerial.read();

    if (c == '\n') {
      lineBuf.trim();
      if (lineBuf.length()) {

        if (lineBuf.indexOf("+UMQTTC: 2,1") >= 0 || lineBuf.indexOf("+UUMQTTC: 2,1") >= 0) {
          PendingSend *f = pendingQFront();
          if (f) {
            if (f->kind == PENDING_MAIN) {
              lastMqttOk = true;
              lastMainSuccessFlag = true;
              lastMainSuccessMs = millis();
              lastMainDurationMs = millis() - mqttAttemptMs;
              mqttLastSuccessMs = millis();
              mqttTimeoutCount = 0;
              lteEverMqtt = true;
              if (f->haveSample) {
                storeSample(f->sample, 1);
                f->haveSample = false;
              }
              if (debugTrace) {
                unsigned long age = mqttAttemptMs ? (millis() - mqttAttemptMs) : 0;
                // Serial.printf("[MQTTURC] SUCCESS (pump) age=%lums\n", age);
              }
              f->line = "";
              f->sampleEpochMs = 0;
            } else if (f->kind == PENDING_BACKLOG) {
              recordBacklogRemoval(f->path, f->line);
              lastMqttOk = true; mqttLastSuccessMs = millis(); mqttTimeoutCount = 0; lteEverMqtt = true;
            }
            pendingQPop();
          }
          static bool rtcPublishSyncDone = false;
          if (!rtcPublishSyncDone && !rtcFirstSynced) {
            rtcPublishSyncDone = true;
          }
        }

        if (lineBuf.indexOf("+UMQTTC: 2,0") >= 0 || lineBuf.indexOf("+UMQTTC: 2,2") >= 0 ||
            lineBuf.indexOf("+UUMQTTC: 2,0") >= 0 || lineBuf.indexOf("+UUMQTTC: 2,2") >= 0) {
          PendingSend *f = pendingQFront();
          if (f) {
            if (f->kind == PENDING_MAIN) {
              lastMqttOk = false; if (mqttTimeoutCount < 250) mqttTimeoutCount++;
              if (f->haveSample) {
                storeSample(f->sample, 0);
                f->haveSample = false;
              }
              if (f->line.length()) {
                if (debugTrace) Serial.println("[MQTTURC] FAIL — backlogging pending payload");
                backlogAppend(f->line, f->sampleEpochMs);
                f->line = "";
              }
              if (debugTrace) {
                unsigned long age = mqttAttemptMs ? (millis() - mqttAttemptMs) : 0;
                Serial.printf("[MQTTURC] FAIL (pump) age=%lums\n", age);
              }
            }
            pendingQPop();
          }
        }

        if (lineBuf.indexOf("+UUMQTTC: 1,1") >= 0) { lteMqttReady = true; lteEverMqtt = true; rtcSyncOnceOnMqttUp(); }
        if (lineBuf.indexOf("+UUMQTTC: 1,0") >= 0) { lteMqttReady = false; }
      }
      lineBuf = "";
    } else if (c != '\r') {

      lineBuf += c;
    }
  }
}

static void lenaGpioInit() {
  if (!lteDetected) return;
  char cmd[40];
  if (LENA_GPIO_NET_STATUS >= 0) {
    snprintf(cmd, sizeof(cmd), "AT+UGPIOC=%d,0,0", LENA_GPIO_NET_STATUS);

  }
  if (LENA_GPIO_FIX_STATUS >= 0) {
    snprintf(cmd, sizeof(cmd), "AT+UGPIOC=%d,0,0", LENA_GPIO_FIX_STATUS);

  }
}

static void lenaGpioSetFix(bool on) {
  if (!lteDetected || LENA_GPIO_FIX_STATUS < 0) return;
  static int last = -1;
  int want = on ? 1 : 0;
  if (last == want) return;
  char cmd[40];
  snprintf(cmd, sizeof(cmd), "AT+UGPIOC=%d,0,%d", LENA_GPIO_FIX_STATUS, want);
  sendCmd(cmd, 150, 1);
  last = want;
}

static void lenaGpioSetMqtt(bool on) {
  if (!lteDetected || LENA_GPIO_NET_STATUS < 0) return;
  static int last = -1;
  int want = on ? 1 : 0;
  if (last == want) return;
  char cmd[40];
  snprintf(cmd, sizeof(cmd), "AT+UGPIOC=%d,0,%d", LENA_GPIO_NET_STATUS, want);
  sendCmd(cmd, 120, 1);
  last = want;
}

static bool initLTE() {

  lteSerial.begin(921600, SERIAL_8N1, PIN_LTE_RX, PIN_LTE_TX);
  delay(1222);
  lteSerial.setRxBufferSize(2048);
#if defined(ESP32)
  lteSerial.onReceive(onLteSerialRx);
#else
  lteSerial.onReceive(onLteSerialRx);
#endif

  if (debugTrace) { Serial.println("Init LTE"); }
  sendCmd("WAIT", 77, 1);
  bool haveAT = lteProbeAT(55);

  if (!haveAT) {

      if (debugTrace) { Serial.println("Resetting LENA-R10"); }

    ltePowerPulse();
    ltePowerPulsesUsed++;
    delay(888);

    sendCmd("WAIT", 300, 1);

    haveAT = lteProbeAT(111);

  }

  if (!haveAT) {

      if (debugTrace) { Serial.println("Resetting LENA-R10"); }

    ltePowerPulse();
    ltePowerPulsesUsed++;
    delay(888);

    sendCmd("WAIT", 300, 1);

    haveAT = lteProbeAT(444);

  }

  if (!haveAT) {
    Serial.println("NO AT response; advancing without LENA.");
    lteDetected = false; lteMqttReady = false;
    return false;
  }
  else if (haveAT){
        Serial.println("LENA RESPONDED.");
  }
  lteDetected = true;
  sendCmd("WAIT", 400, 1);

  sendCmd("AT");
  sendCmd("ATE0");

  sendCmd("AT+CTZU=0", 355, 1);
  if (debugTrace) sendCmd("AT+CFUN?");
  sendCmd("AT+URAT=3", 500, 1);
  if (debugTrace) sendCmd("AT+URAT?");
  sendCmd("AT+CGDCONT=1,\"IP\",\"lte.vodacom.za\"", 20, 1);
  if (debugTrace) sendCmd("AT+CGDCONT?", 200, 2);

  bool attachAttempted = false;
  bool connectAttempted = false;

  if (lteSignalSufficient(true, "initial CGACT")) {
    sendCmd("AT+CGACT=1,1", 20, 1);
    attachAttempted = true;
    delay(50);
  }

  sendCmd("AT+USECPRF=0,0,1", 20, 1);
  sendCmd("AT+USECPRF=0,2,0", 20, 1);
  sendCmd("AT+USECPRF=0,3,\"AWS_CA\"", 20, 1);
  sendCmd("AT+USECPRF=0,5,\"AWS_Client\"", 20, 1);
  sendCmd("AT+USECPRF=0,6,\"Client_Key\"", 20, 1);
  {
    char buf[128];
    snprintf(buf, sizeof(buf), "AT+USECPRF=0,10,\"%s\"", MQTT_HOST); sendCmd(buf, 20, 1);
    snprintf(buf, sizeof(buf), "AT+UMQTT=2,\"%s\",%u", MQTT_HOST, MQTT_PORT_TLS); sendCmd(buf, 20, 1);
  }
  sendCmd("AT+UMQTT=11,1,0", 20, 1);

  if (attachAttempted && lteSignalSufficient(true, "initial MQTT connect")) {
    lastMqttConnectAttemptMs = millis();
    String c = sendCmd("AT+UMQTTC=1", 4000, 2);
    lteMqttReady = (c.indexOf("OK") >= 0 || c.indexOf("UMQTTC") >= 0);
    connectAttempted = true;
  } else {
    lteMqttReady = false;
  }
  if (lteMqttReady) {
    lteEverMqtt = true;
    rtcSyncOnceOnMqttUp();
  }

  const char* initStatus = lteMqttReady ? "connected" : (connectAttempted ? "failed" : "skipped (CSQ)");
  Serial.printf("[MQTT] initial connect: %s\n", initStatus);
  return true;
}

static bool ensureLTEConnected(){
  if (!lteDetected) {
    return false;
  }
  unsigned long now = millis();
  if (lteMqttReady) return true;

  if (mqttLastSuccessMs && (now - mqttLastSuccessMs) < MQTT_RECONNECT_GRACE_MS) {
    return false;
  }
  if (lastMqttConnectAttemptMs && (now - lastMqttConnectAttemptMs) < MQTT_CONNECT_MIN_INTERVAL_MS) {
    return false;
  }

  if (!lteSignalSufficient(true, "attach retry")) {
    return false;
  }

  String a = sendCmd("AT+CGACT=1,1", 300, 1);
  bool attachOk = (a.indexOf("OK") >= 0);
  if (!attachOk) {
    if (debugTrace) {
      String ta = a; ta.trim();
      Serial.printf("[LTEWARN] CGACT failed: '%s'\n", ta.c_str());
    }
    return false;
  }

  if (!lteSignalSufficient(false, "MQTT connect retry")) {
    return false;
  }

  lastMqttConnectAttemptMs = millis();
  String b = sendCmd("AT+UMQTTC=1", 500, 1);
  bool ok = (b.indexOf("OK") >= 0) || (b.indexOf("UMQTTC") >= 0);
  lteMqttReady = ok;
  if (ok) {
    lteEverMqtt = true;
    if (debugTrace) { Serial.println("[LTE] MQTT connected"); }
    rtcSyncOnceOnMqttUp();
  } else if (debugTrace) {
    String tb = b; tb.trim();
    Serial.printf("[LTEWARN] UMQTTC=1 failed: '%s'\n", tb.c_str());
  }
  return lteMqttReady;
}

static bool publishHexPayload(const String &hexPayload, const String &fallbackHex, const Sample &sample) {
  if (!lteDetected) { lastMqttOk = false; mqttPending = (pendingMainCount > 0); return false; }
  if (pendingQIsFull()) {
    if (debugTrace) Serial.println("[MQTT] Pending queue full — skipping publish");
    return false;
  }

  if (!lteSignalSufficient(true, "publish precheck")) {
    lastMqttOk = false; mqttPending = (pendingMainCount > 0); return false;
  }

  if (!lteMqttReady) {
    (void)ensureLTEConnected();
    if (!lteMqttReady) {
      lastMqttOk = false; mqttPending = (pendingMainCount > 0); return false;
    }
  }

  char atCmd[800];
  snprintf(atCmd, sizeof(atCmd),
           "AT+UMQTTC=2,1,0,1,\"%s\",\"%s\"",
           MQTT_PUB_TOPIC, hexPayload.c_str());

  static unsigned long seq = 0; seq++;
  (void)seq;
  String r = sendCmd(atCmd, 500, 1);
  (void)r;
  lastMqttOk = false;
  mqttAttemptMs = millis();
  if (!pendingQPushMain(mqttAttemptMs, PENDING_MAIN_TIMEOUT_MS, fallbackHex, sample)) {
    if (debugTrace) Serial.println("[MQTT] Pending queue full — dropping to backlog");
    return false;
  }

  lastMainAttemptValid = true;
  lastMainAttemptMs = mqttAttemptMs;
  lastMainSuccessFlag = false;
  return true;
}

static bool publishHexPayloadBacklogEnqueue(const char* path, const String &lineHex) {
  if (!lteDetected) { if (debugTrace) Serial.println("[FLUSH] LTE not detected"); return false; }
  if (!lteMqttReady) { if (debugTrace) Serial.println("[FLUSH] MQTT not ready"); return false; }
  if (pendingQIsFull()) {
    if (debugTrace) Serial.println("[FLUSH] Pending queue full — deferring backlog publish");
    return false;
  }
  if (!lteSignalSufficient(true, "backlog publish")) {
    if (debugTrace) Serial.println("[FLUSH] CSQ gate blocked backlog publish");
    return false;
  }
  char atCmd[800];
  snprintf(atCmd, sizeof(atCmd), "AT+UMQTTC=2,1,0,1,\"%s\",\"%s\"", MQTT_PUB_TOPIC, lineHex.c_str());
  (void)sendCmd(atCmd, 500, 1);
  if (!pendingQPushBacklog(path, lineHex, millis(), 3000)) {
    if (debugTrace) Serial.println("[FLUSH] Pending queue push failed — keeping backlog entry");
    return false;
  }
  if (debugTrace) {
    Serial.printf("[FLUSH] Backlog publish queued: %s\n", path ? path : "(unknown)");
  }
  return true;
}

static void gnssEnableLastNmea() {

  sendCmd("AT+UGGGA=1", 200, 1);
  sendCmd("AT+UGGSA=1", 200, 1);

  sendCmd("AT+UGRMC=1", 200, 1);
}

static String gnssFetchLast(const char* cmd, const char* tag, uint8_t &missCounter,
                            unsigned long timeoutMs, int expectedLines) {
  String resp = sendCmd(cmd, timeoutMs, expectedLines);
  if (resp.indexOf(tag) >= 0) {
    if (missCounter && debugTrace) {
      Serial.printf("[GNSS] %s recovered after %u misses\n", tag, missCounter);
    }
    missCounter = 0;
    return resp;
  }

  missCounter++;
  String raw = resp;
  raw.trim();
  raw.replace('\n', '|');
  raw.replace('\r', '|');
  if (raw.length() > 80) {
    raw = raw.substring(0, 80) + "…";
  }

  if (missCounter == 1) {
    gnssEnableLastNmea();

    resp = sendCmd(cmd, timeoutMs, expectedLines);
    if (resp.indexOf(tag) >= 0) {
      if (debugTrace) {
        Serial.printf("[GNSS] %s recovered immediately after NMEA re-enable\n", tag);
      }
      missCounter = 0;
      return resp;
    }

  } else if (missCounter == 3) {
    sendCmd("AT+UGPS=1,1,1", 500, 1);
  }
  return resp;
}

namespace ADS1220 {
  static SPISettings spiCfg(500000, MSBFIRST, SPI_MODE1);
  enum : uint8_t {
    CMD_RESET = 0x06,
    CMD_START = 0x08,
    CMD_POWER = 0x02,
    CMD_RDATA = 0x10,
    REG0 = 0x00, REG1 = 0x01, REG2 = 0x02, REG3 = 0x03
  };
  inline uint8_t WREG(uint8_t addr, uint8_t nbytes){ return 0x40 | ((addr & 0x03) << 2) | ((nbytes - 1) & 0x03); }
  inline uint8_t RREG(uint8_t addr, uint8_t nbytes){ return 0x20 | ((addr & 0x03) << 2) | ((nbytes - 1) & 0x03); }

  enum : uint8_t { MUX_0_AVSS = 0x3 };

  static inline void csLow(){ digitalWrite(PIN_ADS_CS, LOW); }
  static inline void csHigh(){ digitalWrite(PIN_ADS_CS, HIGH); }
static void sendCmd(uint8_t cmd){
  SPI.beginTransaction(spiCfg);
  csLow(); delayMicroseconds(2);
  SPI.transfer(cmd);
  delayMicroseconds(2); csHigh();
  SPI.endTransaction();
}
static void writeReg(uint8_t addr, uint8_t val){
  SPI.beginTransaction(spiCfg);
  csLow(); delayMicroseconds(2);
  SPI.transfer(WREG(addr,1)); SPI.transfer(val);
  delayMicroseconds(2); csHigh();
  SPI.endTransaction();
}
static uint8_t readReg(uint8_t addr){
  SPI.beginTransaction(spiCfg);
  csLow(); delayMicroseconds(2);
  SPI.transfer(RREG(addr,1)); uint8_t v=SPI.transfer(0x00);
  delayMicroseconds(2); csHigh();
  SPI.endTransaction();
  return v;
}
  static int32_t readData24(){
  SPI.beginTransaction(spiCfg);
  csLow(); delayMicroseconds(2);
  SPI.transfer(CMD_RDATA);
  uint32_t b0=SPI.transfer(0), b1=SPI.transfer(0), b2=SPI.transfer(0);
  delayMicroseconds(2); csHigh();
  SPI.endTransaction();
  int32_t v=(int32_t)((b0<<16)|(b1<<8)|b2); if(v & 0x800000) v|=0xFF000000;
  return v;
  }
}

void initCurrentSensorADS() {
  if (debugTrace) {
    Serial.println("[ADS1220] ===== init begin =====");

  }
  pinMode(PIN_ADS_CS, OUTPUT); digitalWrite(PIN_ADS_CS, HIGH);
  pinMode(PIN_ADS_DRDY, INPUT);
  ADS1220::sendCmd(ADS1220::CMD_RESET);
  delay(2);
  if (debugTrace) Serial.println("[ADS1220] reset done");

  uint8_t reg0 = (0x3<<4) | (0u<<1) | 0x0;
  ADS1220::writeReg(ADS1220::REG0, reg0);

    uint8_t reg1 = (0b000<<5) | (0b10<<3) | (1<<2) | 0b00;

  ADS1220::writeReg(ADS1220::REG1, reg1);

  uint8_t reg2 = (0x00);
  ADS1220::writeReg(ADS1220::REG2, reg2);
  ADS1220::writeReg(ADS1220::REG3, 0x00);

  uint8_t rd0 = ADS1220::readReg(ADS1220::REG0);
  uint8_t rd1 = ADS1220::readReg(ADS1220::REG1);
  uint8_t rd2 = ADS1220::readReg(ADS1220::REG2);
  uint8_t rd3 = ADS1220::readReg(ADS1220::REG3);
  adsPresent = (rd0 == reg0);
  if (!adsPresent) {
    if (debugTrace) {
      Serial.printf("[ADS1220] presence check FAILED\n");
      Serial.printf("[ADS1220] REG0 want=0x%02X got=0x%02X\n", reg0, rd0);
      Serial.printf("[ADS1220] REG1 want=0x%02X got=0x%02X\n", reg1, rd1);
      Serial.printf("[ADS1220] REG2 want=0x%02X got=0x%02X\n", reg2, rd2);
      Serial.printf("[ADS1220] REG3 want=0x%02X got=0x%02X\n", 0,    rd3);
      Serial.printf("[ADS1220] DRDY pin state=%d (expect HIGH idle)\n", digitalRead(PIN_ADS_DRDY));
      Serial.println("[ADS1220] Disabling ADS reads.");
    }
  } else {
    if (debugTrace) {
      Serial.println("[ADS1220] presence check OK");
      Serial.printf("[ADS1220] REG0/1/2/3 = %02X %02X %02X %02X\n", rd0, rd1, rd2, rd3);
      Serial.println("[ADS1220] Starting conversions…");
    }
  }

  if (adsPresent) {
    ADS1220::sendCmd(ADS1220::CMD_START);
  }
  adcBusIdle();
  if (debugTrace) Serial.println("[ADS1220] ===== init end =====");
}

namespace AMC {

  static inline void clkin_begin_disabled(){ ledcAttach(PIN_CLKIN, 8'000'000, 1); ledcWrite(PIN_CLKIN, 0); }
  static inline void clkin_enable() { ledcWrite(PIN_CLKIN, 1); }
  static inline void clkin_disable(){ ledcWrite(PIN_CLKIN, 0); }

  static SPISettings spiCfg(500000, MSBFIRST, SPI_MODE1);

  enum : uint16_t { CMD_NULL=0x0000, CMD_RESET=0x0011, CMD_STBY=0x0022, CMD_WAKEUP=0x0033, CMD_LOCK=0x0555, CMD_UNLOCK=0x0655 };
  static inline uint16_t CMD_RREG(uint8_t addr,uint8_t n_minus_1=0){ return uint16_t(0xA000u | ((addr & 0x3Fu)<<7) | (n_minus_1 & 0x7Fu)); }
  static inline uint16_t CMD_WREG(uint8_t addr,uint8_t n_minus_1=0){ return uint16_t(0x6000u | ((addr & 0x3Fu)<<7) | (n_minus_1 & 0x7Fu)); }
  enum : uint8_t {
    REG_ID=0x00, REG_STATUS=0x01, REG_MODE=0x02, REG_CLOCK=0x03, REG_GAIN=0x04,
    REG_CFG=0x06,
    REG_DCDC_CTRL=0x31
  };
  namespace MODE { constexpr uint16_t WLENGTH_SHIFT=8; constexpr uint16_t DRDY_HIZ=1u<<1; constexpr uint16_t DRDY_FMT=1u<<0; constexpr uint16_t RESET_BIT=1u<<10; }
  namespace CLOCK{ constexpr uint16_t CH0_EN=1u<<8; constexpr uint16_t CLK_DIV_SHIFT=6; constexpr uint16_t OSR_SHIFT=2; constexpr uint16_t PWR_SHIFT=0; }
  namespace DCDC { constexpr uint16_t FREQ_SHIFT=8; constexpr uint16_t EN=1u<<0; }
  namespace CFG  { constexpr uint16_t GC_EN=1u<<0; constexpr uint16_t GC_DLY_SHIFT=4; constexpr uint16_t GC_DLY_MASK=(0xFu<<GC_DLY_SHIFT); }

  namespace GAIN {

    constexpr uint16_t PGA_SHIFT = 0;
    constexpr uint16_t PGA_MASK  = 0x0007u;

    constexpr uint8_t  PGA_1     = 0x0;
    constexpr uint8_t  PGA_2     = 0x1;
    constexpr uint8_t  PGA_4     = 0x2;
    constexpr uint8_t  PGA_8     = 0x3;
  }

  static inline uint32_t pack16to24(uint16_t w){ return (uint32_t)w<<8; }
  static inline uint32_t xfer24(uint32_t tx){
    uint8_t b2=(tx>>16)&0xFF, b1=(tx>>8)&0xFF, b0=tx&0xFF;
    uint8_t r2=SPI.transfer(b2), r1=SPI.transfer(b1), r0=SPI.transfer(b0);
    return (uint32_t)r2<<16 | (uint32_t)r1<<8 | r0;
  }
  struct FrameRX{ uint32_t w0,w1,w2; };
  static FrameRX frame3(uint32_t w0,uint32_t w1=0,uint32_t w2=0){ FrameRX r{}; digitalWrite(PIN_VOLT_CS,LOW); r.w0=xfer24(w0); r.w1=xfer24(w1); r.w2=xfer24(w2); digitalWrite(PIN_VOLT_CS,HIGH); return r; }
  static inline bool drdy_active(){ return digitalRead(PIN_VOLT_DRDY)==LOW; }
  static inline int32_t sx24(uint32_t v){ return (v&0x800000u)?int32_t(v|0xFF000000u):int32_t(v); }
  static uint16_t readReg16(uint8_t addr){ frame3(pack16to24(CMD_RREG(addr,0))); FrameRX f=frame3(pack16to24(CMD_NULL)); return uint16_t(f.w0>>8); }
  static void writeReg16(uint8_t addr,uint16_t val){ frame3(pack16to24(CMD_WREG(addr,0)), pack16to24(val), 0); (void)frame3(pack16to24(CMD_NULL)); }
}

void initVoltageSensor() {
  pinMode(PIN_ADS_CS, OUTPUT);
  digitalWrite(PIN_ADS_CS, HIGH);
  delay(1000);
  if (debugTrace) {
    Serial.println("[AMC] ===== init begin =====");

  }
  pinMode(PIN_VOLT_CS, OUTPUT);
  digitalWrite(PIN_VOLT_CS, HIGH);
  digitalWrite(PIN_ADS_CS, HIGH);
  pinMode(PIN_VOLT_DRDY, INPUT);
  delay(50);

  AMC::clkin_begin_disabled();
  if (debugTrace) Serial.println("[AMC] CLKIN attached (disabled)");

  SPI.beginTransaction(AMC::spiCfg);
  (void)AMC::frame3(AMC::pack16to24(AMC::CMD_RESET));
  (void)AMC::frame3(AMC::pack16to24(AMC::CMD_NULL));
  (void)AMC::frame3(AMC::pack16to24(AMC::CMD_UNLOCK));

  uint16_t mode = AMC::readReg16(AMC::REG_MODE);
  mode &= ~(0b11u << AMC::MODE::WLENGTH_SHIFT);
  mode |= (0b01u << AMC::MODE::WLENGTH_SHIFT);
  mode |= AMC::MODE::DRDY_HIZ;
  mode &= ~AMC::MODE::DRDY_FMT;
  mode &= ~AMC::MODE::RESET_BIT;
  AMC::writeReg16(AMC::REG_MODE, mode);
  if (debugTrace) Serial.printf("[AMC] MODE=0x%04X\n", mode);

  uint16_t gain = AMC::readReg16(AMC::REG_GAIN);
  gain &= ~AMC::GAIN::PGA_MASK;
  gain |= (uint16_t(AMC::GAIN::PGA_1) << AMC::GAIN::PGA_SHIFT);
  AMC::writeReg16(AMC::REG_GAIN, gain);
  if (debugTrace) Serial.printf("[AMC] GAIN=0x%04X\n", gain);

  constexpr uint8_t CLK_DIV_CODE = 0b00;
  constexpr uint8_t OSR_CODE = 0b111;
  constexpr uint8_t PWR_CODE = 0b00;
  uint16_t clock = AMC::CLOCK::CH0_EN
                 | (uint16_t(CLK_DIV_CODE) << AMC::CLOCK::CLK_DIV_SHIFT)
                 | (uint16_t(OSR_CODE)   << AMC::CLOCK::OSR_SHIFT)
                 | (uint16_t(PWR_CODE)   << AMC::CLOCK::PWR_SHIFT);
  AMC::writeReg16(AMC::REG_CLOCK, clock);
  if (debugTrace) Serial.printf("[AMC] CLOCK=0x%04X\n", clock);

  uint16_t cfg = AMC::readReg16(AMC::REG_CFG);
  cfg |= AMC::CFG::GC_EN;
  cfg = (cfg & ~AMC::CFG::GC_DLY_MASK);
  AMC::writeReg16(AMC::REG_CFG, cfg);
  if (debugTrace) Serial.printf("[AMC] CFG=0x%04X\n", cfg);

  constexpr uint8_t DCDC_FREQ_CODE = 0b0;
  uint16_t dcdc = (uint16_t(DCDC_FREQ_CODE) << AMC::DCDC::FREQ_SHIFT);
  AMC::writeReg16(AMC::REG_DCDC_CTRL, dcdc);
  AMC::writeReg16(AMC::REG_DCDC_CTRL, uint16_t(dcdc | AMC::DCDC::EN));
  SPI.endTransaction();
  if (debugTrace) {
    uint16_t r_mode  = AMC::readReg16(AMC::REG_MODE);
    uint16_t r_clock = AMC::readReg16(AMC::REG_CLOCK);
    uint16_t r_cfg   = AMC::readReg16(AMC::REG_CFG);
    Serial.printf("[AMC] Verify MODE=0x%04X CLOCK=0x%04X CFG=0x%04X\n", r_mode, r_clock, r_cfg);
  }

  AMC::clkin_enable();
  delay(200);
  amcDiscard = 5;
  adcBusIdle();
  if (debugTrace) Serial.println("[AMC] CLKIN enabled; init end");
}

namespace MCP2515 {

  static SPISettings spiCfg(1'000'000, MSBFIRST, SPI_MODE0);

  enum : uint8_t {
    CANSTAT=0x0E, CANCTRL=0x0F,
    CNF3=0x28, CNF2=0x29, CNF1=0x2A,
    CANINTE=0x2B, CANINTF=0x2C,
    RXB0CTRL=0x60, RXB1CTRL=0x70
  };

  enum : uint8_t {
    CMD_RESET=0xC0,
    CMD_READ=0x03,
    CMD_WRITE=0x02,
    CMD_BITMOD=0x05,
    CMD_READ_STATUS=0xA0,
    CMD_READ_RX0=0x90,
    CMD_READ_RX1=0x94
  };

  static inline void csLow(int cs){ digitalWrite(cs, LOW); }
  static inline void csHigh(int cs){ digitalWrite(cs, HIGH); }

  static inline void reset(int cs){ hspi.beginTransaction(spiCfg); csLow(cs); hspi.transfer(CMD_RESET); csHigh(cs); hspi.endTransaction(); }
  static uint8_t readReg(int cs, uint8_t addr){ hspi.beginTransaction(spiCfg); csLow(cs); hspi.transfer(CMD_READ); hspi.transfer(addr); uint8_t v=hspi.transfer(0); csHigh(cs); hspi.endTransaction(); return v; }
  static void writeReg(int cs, uint8_t addr, uint8_t val){ hspi.beginTransaction(spiCfg); csLow(cs); hspi.transfer(CMD_WRITE); hspi.transfer(addr); hspi.transfer(val); csHigh(cs); hspi.endTransaction(); }
  static void bitModify(int cs, uint8_t addr, uint8_t mask, uint8_t data){ hspi.beginTransaction(spiCfg); csLow(cs); hspi.transfer(CMD_BITMOD); hspi.transfer(addr); hspi.transfer(mask); hspi.transfer(data); csHigh(cs); hspi.endTransaction(); }

  static void configTiming500k_8MHz(int cs){

    writeReg(cs, CNF1, 0x00);

    writeReg(cs, CNF2, 0x90);

    writeReg(cs, CNF3, 0x02);
  }

  static bool setMode(int cs, uint8_t mode){

    bitModify(cs, CANCTRL, 0xE0, (uint8_t)(mode<<5));

    uint32_t t0=millis();
    while (millis()-t0 < 50) {
      uint8_t stat = readReg(cs, CANSTAT);
      if (((stat>>5)&0x07) == mode) return true;
    }
    return false;
  }

  struct CanFrame {
    uint16_t id; bool extended; uint8_t dlc; uint8_t data[8];
  };

  static bool readFrame(int cs, CanFrame &out){
    uint8_t intf = readReg(cs, CANINTF);
    if (intf & 0x01) {

      hspi.beginTransaction(spiCfg); csLow(cs); hspi.transfer(CMD_READ_RX0);
      uint8_t sidh=hspi.transfer(0), sidl=hspi.transfer(0);
      (void)hspi.transfer(0); (void)hspi.transfer(0);
      uint8_t dlc=hspi.transfer(0) & 0x0F;
      for (uint8_t i=0;i<dlc;i++) out.data[i]=hspi.transfer(0);
      for (uint8_t i=dlc;i<8;i++) (void)hspi.transfer(0);
      csHigh(cs); hspi.endTransaction();
      out.extended = (sidl & 0x08);
      out.id = (uint16_t(sidh)<<3) | (sidl>>5);
      out.dlc = dlc;
      bitModify(cs, CANINTF, 0x01, 0x00);
      return true;
    }
    if (intf & 0x02) {

      hspi.beginTransaction(spiCfg); csLow(cs); hspi.transfer(CMD_READ_RX1);
      uint8_t sidh=hspi.transfer(0), sidl=hspi.transfer(0);
      (void)hspi.transfer(0); (void)hspi.transfer(0);
      uint8_t dlc=hspi.transfer(0) & 0x0F;
      for (uint8_t i=0;i<dlc;i++) out.data[i]=hspi.transfer(0);
      for (uint8_t i=dlc;i<8;i++) (void)hspi.transfer(0);
      csHigh(cs); hspi.endTransaction();
      out.extended = (sidl & 0x08);
      out.id = (uint16_t(sidh)<<3) | (sidl>>5);
      out.dlc = dlc;
      bitModify(cs, CANINTF, 0x02, 0x00);
      return true;
    }
    return false;
  }

  static bool init(int cs){
    pinMode(cs, OUTPUT); digitalWrite(cs, HIGH);

    reset(cs); delay(5);
    if (!setMode(cs, 0b100)) return false;

    configTiming500k_8MHz(cs);

    writeReg(cs, RXB0CTRL, 0x60);
    writeReg(cs, RXB1CTRL, 0x60);

    writeReg(cs, CANINTE, 0x03);

    if (!setMode(cs, 0b000)) return false;
    return true;
  }
}

void initFluxgateCanBus() {
  pinMode(PIN_CAN2_CS, OUTPUT);
  pinMode(PIN_CAN2_INT, INPUT);
  digitalWrite(PIN_CAN2_CS, HIGH);
  if (!MCP2515::init(PIN_CAN2_CS)) {
    Serial.println("MCP2515 (flux) init failed — check wiring and CNF timing.");
  }
}

void initVehicleCanBus() {
  pinMode(PIN_CAN1_CS, OUTPUT);
  pinMode(PIN_CAN1_INT, INPUT);
  digitalWrite(PIN_CAN1_CS, HIGH);
  if (!MCP2515::init(PIN_CAN1_CS)) {
    Serial.println("MCP2515 (vehicle) init failed — check wiring and CNF timing.");
  }
}

void initAccelerometer() {
  if (!accel.begin()) {
    Serial.println("Accelerometer not detected; motion data disabled.");
  } else {
    accel.setRange(ADXL343_RANGE_2_G);
  }
}

void initTempSensor() {
  tempSensor.begin();

  tempSensor.setWaitForConversion(false);
}

void initRTC() {
  Serial.println("[RTC] Initializing MCP7940...");
  rtcI2C.beginTransmission(0x6F);
  if (rtcI2C.endTransmission() != 0) { Serial.println("[RTC] ERROR: MCP7940N not found"); return; }

  uint8_t sec=0; if (rtcReadRegs(0x00, &sec, 1)) {
    if (!(sec & 0x80)) { sec |= 0x80; rtcWriteReg(0x00, sec); delay(10); }
  }

  uint8_t wk=0; if (rtcReadRegs(0x03, &wk, 1)) { wk |= (1<<3); if ((wk & 0x07)==0) wk |= 0x01; rtcWriteReg(0x03, wk); }

  uint16_t y; uint8_t mo,d,wd,h,m,s;
  if (rtcGetDateTime(y,mo,d,wd,h,m,s)) {
    Serial.printf("[RTC] Current time: %04u-%02u-%02u %02u:%02u:%02u\n", y,mo,d,h,m,s);

    rtcFirstSynced = false;
  }
}

void printRtcDiagnostics() {
  if (!debugTrace) return;

  static unsigned long lastRtcDiag = 0;
  if ((millis() - lastRtcDiag) < 30000) return;
  lastRtcDiag = millis();

  Serial.println("[RTC DIAGNOSTICS]");

  uint8_t regs[8];
  if (rtcReadRegs(0x00, regs, 8)) {
    Serial.print("[RTC] Registers 0x00-0x07:");
    for (int i = 0; i < 8; i++) {
      Serial.printf(" %02X", regs[i]);
    }
    Serial.println();

    Serial.printf("[RTC] ST(osc): %s, 12/24: %s\n",
                  (regs[0] & 0x80) ? "ON" : "OFF",
                  (regs[2] & 0x40) ? "12hr" : "24hr");
  } else {
    Serial.println("[RTC] ERROR: Cannot read registers");
  }

  uint16_t y; uint8_t mon, d, wd, h, m, s;
  if (rtcGetDateTime(y, mon, d, wd, h, m, s)) {
    Serial.printf("[RTC] Time: %04d-%02d-%02d %02d:%02d:%02d\n", y, mon, d, h, m, s);
    uint32_t epoch = rtcGetEpoch();
    Serial.printf("[RTC] Epoch: %lu\n", (unsigned long)epoch);
  } else {
    Serial.println("[RTC] ERROR: Cannot read time");
  }
}

void initLteGnss() {

  lteDetected = initLTE();
  if (lteDetected) {

    lenaGpioInit();
  }

  sendCmd("AT+UGPS=1,1,197", 500, 1);

  if (debugTrace) sendCmd("AT+UGPS?", 500, 2);

  gnssEnableLastNmea();
}

void readVoltage() {
if (!AMC::drdy_active()) return;

SPI.beginTransaction(AMC::spiCfg);
AMC::FrameRX f = AMC::frame3(AMC::pack16to24(AMC::CMD_NULL));
SPI.endTransaction();

uint16_t status = uint16_t(f.w0 >> 8);
bool sec_ok = ((status & (1u<<6))==0);
bool drdy_b = (status & 0x0001);
if (!(sec_ok && drdy_b)) return;

if (amcDiscard) { uint8_t tmp = amcDiscard; amcDiscard = (uint8_t)(tmp - 1); return; }

int32_t code = AMC::sx24(f.w1 & 0xFFFFFFu);

constexpr double LSB_V = 1.2 / 8388608.0;
double vAdc = double(code) * LSB_V;
double input_imp = 560;
double gain_factor = 3 / (3*input_imp / (3+input_imp));

double vPack = (vAdc / double(AMC_GAIN)) * double(HV_DIV_RATIO) *double(gain_factor);
aggregator.sumVoltage += float(vPack);
aggregator.countVoltage++;
adcBusIdle();

}

static void printAveragesIfReady() {
  static unsigned long lastPrintMs = 0;
  unsigned long now = millis();
  if (aggregator.countVoltage == 0 && aggregator.countCurrentAds == 0) return;
  if (now - lastPrintMs < 500) return;
  float avgVolt = aggregator.countVoltage
                    ? float(aggregator.sumVoltage / double(aggregator.countVoltage))
                    : NAN;
  float avgCurr = aggregator.countCurrentAds
                    ? float(aggregator.sumCurrentAds / double(aggregator.countCurrentAds))
                    : NAN;
  Serial.printf("[MEAS] V=%.3f V  I=%.3f A  counts(V=%lu I=%lu)\n",
                avgVolt, avgCurr,
                aggregator.countVoltage, aggregator.countCurrentAds);
  aggregator.reset();
  lastPrintMs = now;
}

void readCurrentADS() {
  if (!adsPresent) return;

  static unsigned long lastPollMs = 0;
  unsigned long nowMs = millis();
  if (nowMs - lastPollMs < ADS_READ_INTERVAL_MS) return;
  lastPollMs = nowMs;

  static int lastDrdy = HIGH;

    int32_t code = ADS1220::readData24();
    float vAdc = convertAdsCountsToVolts(code);
    float current = vAdc * CURRENT_A_PER_V;
    aggregator.sumCurrentAds += current;
    aggregator.countCurrentAds++;
    if (debugTrace) {
      uint32_t raw24 = (uint32_t)(code & 0x00FFFFFFu);
      double mv = vAdc * 1000.0;

    }

}

void readFluxgateCAN() {
  MCP2515::CanFrame f{};
  if (MCP2515::readFrame(PIN_CAN2_CS, f)) {
    aggregator.fluxCanCount++;
    if (f.dlc >= (FLUX_CURR_BYTE_OFFSET + 3) && (FLUX_CURR_CAN_ID < 0 || f.id == (uint16_t)FLUX_CURR_CAN_ID)) {

      uint32_t raw = (uint32_t(f.data[FLUX_CURR_BYTE_OFFSET+0])<<16) |
                     (uint32_t(f.data[FLUX_CURR_BYTE_OFFSET+1])<<8)  |
                      uint32_t(f.data[FLUX_CURR_BYTE_OFFSET+2]);
      lastFluxRaw24 = raw;
      float current = convertFluxRawToCurrent(raw);
      aggregator.sumCurrentFlux += current;
      aggregator.countCurrentFlux++;

    }
  }
}

void readVehicleCAN() {
  MCP2515::CanFrame f{};
  if (MCP2515::readFrame(PIN_CAN1_CS, f)) {
    aggregator.vehCanCount++;
  }
}

void readAccelerometer() {
  sensors_event_t event;
  accel.getEvent(&event);
  if (isnan(event.acceleration.x)) {
    return;
  }
  aggregator.sumAccelX += event.acceleration.x;
  aggregator.sumAccelY += event.acceleration.y;
  aggregator.sumAccelZ += event.acceleration.z;
  aggregator.countAccel++;
}

void readTemperature() {
  static bool convRequested = false;
  static unsigned long lastReqMs = 0;
  unsigned long now = millis();

  if (!convRequested || (now - lastReqMs) >= 1100) {
    tempSensor.requestTemperatures();
    lastReqMs = now;
    convRequested = true;
    return;
  }

  if (convRequested && tempSensor.isConversionComplete()) {
    float t = tempSensor.getTempCByIndex(0);
    if (t > -127.0f) {
      aggregator.sumTemp += t;
      aggregator.countTemp++;
    }
    convRequested = false;
  }
}

void readGnss() {
  auto parseDegMin = [](const String &dm)->double{
    if (dm.length() < 3) return NAN;
    double v = dm.toFloat();
    int dd = int(v / 100.0);
    double mm = v - dd*100.0;
    return double(dd) + mm/60.0;
  };
  while (lteSerial.available()) {
    String line = lteSerial.readStringUntil('\n');
    line.trim();

    if (line.startsWith("$GPGGA") || line.startsWith("$GNGGA")) {
      String f[16]; int nf=0; int start=0;
      for (int i=0;i<=line.length() && nf<16;i++){
        if (i==line.length() || line.charAt(i)==',') { f[nf++] = line.substring(start, i); start=i+1; }
      }
      if (nf >= 10) {

        if (f[2].length() && f[4].length()) {
          double lat = parseDegMin(f[2]); if (f[3] == "S") lat = -lat;
          double lon = parseDegMin(f[4]); if (f[5] == "W") lon = -lon;
          aggregator.gnssLat = lat; aggregator.gnssLon = lon;
        }

        int sats = f[7].length() ? f[7].toInt() : 0;
        float hdop = f[8].length() ? f[8].toFloat() : 0.0f;
        aggregator.gnssSats = sats;
        aggregator.gnssHdop = hdop;
        if (f[9].length()) aggregator.gnssAlt = f[9].toFloat();

        aggregator.gnssValid = (gnssFixDim >= 3);
        gnssFixAvailable = aggregator.gnssValid;
      }
    }

    if (line.startsWith("$GPGSA") || line.startsWith("$GNGSA")) {
      String f[18]; int nf=0; int start=0;
      for (int i=0;i<=line.length() && nf<18;i++){
        if (i==line.length() || line.charAt(i)==',') { f[nf++] = line.substring(start, i); start=i+1; }
      }
      if (nf >= 3) {
        int fixType = f[2].length() ? f[2].toInt() : 0;
        if (fixType >= 0 && fixType <= 3) gnssFixDim = (uint8_t)fixType;
      }
    }

    if (line.startsWith("$GPVTG") || line.startsWith("$GNVTG")) {
      String f[12]; int nf=0; int start=0;
      for (int i=0;i<=line.length() && nf<12;i++){
        if (i==line.length() || line.charAt(i)==',') { f[nf++] = line.substring(start, i); start=i+1; }
      }
      if (nf >= 8) {
        double newHeading = NAN; if (f[1].length()) newHeading = f[1].toFloat();
        double spd = NAN;
        if (f[7].length()) spd = f[7].toFloat() / 3.6f;
        else if (f[5].length()) spd = f[5].toFloat() * 0.514444f;
        aggregator.gnssSpeed = spd;
        aggregator.gnssHeading = (isfinite(spd) && spd > MIN_HEADING_SPEED_MS) ? newHeading : NAN;
      }
    }
    if (line.startsWith("$GPRMC") || line.startsWith("$GNRMC")) {

      String f[16]; int nf=0; int start=0;
      for (int i=0;i<=line.length() && nf<16;i++){
        if (i==line.length() || line.charAt(i)==',') { f[nf++] = line.substring(start, i); start=i+1; }
      }
      if (nf >= 10 && f[2] == "A") {

        double lat = parseDegMin(f[3]); if (f[4] == "S") lat = -lat;
        double lon = parseDegMin(f[5]); if (f[6] == "W") lon = -lon;
        aggregator.gnssLat = lat; aggregator.gnssLon = lon;
        double spd = f[7].length() ? f[7].toFloat() * 0.514444 : 0.0;
        double newHeading = f[8].length() ? f[8].toFloat() : NAN;
        aggregator.gnssSpeed = spd;
        aggregator.gnssHeading = (isfinite(spd) && spd > MIN_HEADING_SPEED_MS) ? newHeading : NAN;
        aggregator.gnssValid = (gnssFixDim >= 3); gnssFixAvailable = true;

        String t = f[1]; String d = f[9];
        if (t.length() >= 6 && d.length() == 6) {
          int hh = t.substring(0,2).toInt();
          int mm = t.substring(2,4).toInt();
          int ss = t.substring(4,6).toInt();
          int day = d.substring(0,2).toInt();
          int mon = d.substring(2,4).toInt();
          int yy  = d.substring(4,6).toInt();
          int year = 2000 + yy;
          struct tm tmv{}; tmv.tm_year = year - 1900; tmv.tm_mon = mon - 1; tmv.tm_mday = day;
          tmv.tm_hour = hh; tmv.tm_min = mm; tmv.tm_sec = ss;

          auto tmToEpochUTC = [](const struct tm& t)->time_t{
            int Y = t.tm_year + 1900; int M = t.tm_mon + 1; int D = t.tm_mday;
            int hh = t.tm_hour; int mm = t.tm_min; int ss = t.tm_sec;
            int y = Y - (M <= 2);
            int era = (y >= 0 ? y : y - 399) / 400;
            unsigned yoe = (unsigned)(y - era * 400);
            unsigned doy = (unsigned)((153 * (M + (M > 2 ? -3 : 9)) + 2)/5 + D - 1);
            unsigned doe = yoe*365 + yoe/4 - yoe/100 + doy;
            int64_t days = (int64_t)era*146097 + (int64_t)doe - 719468;
            return (time_t)(days*86400LL + hh*3600 + mm*60 + ss);
          };
          time_t epoch = tmToEpochUTC(tmv);
          if (epoch > 0) {
            gnssEpochMs = (uint64_t)epoch * 1000ULL;
            gnssTimeAnchorMillis = millis();
            gnssTimeAvailable = true;

            if (!rtcFirstSynced) {
              rtcSyncFromEpoch((time_t)(gnssEpochMs / 1000ULL));
              rtcFirstSynced = true;
              if (debugTrace) Serial.println("[TIME] RTC synced from GNSS (first sync)");
            }
          }
        }
      }
    }
  }

  if (debugTrace && gnssFixDim < 3) {
    static unsigned long lastNoFixMs = 0; unsigned long now = millis();
    if (now - lastNoFixMs >= 1000) {
      lastNoFixMs = now;
      Serial.printf("[GNSS] no fix; sats=%d hdop=%.1f\n", aggregator.gnssSats, (double)aggregator.gnssHdop);
    }
  }
}

void readSensors() {
  readVoltage();
  readCurrentADS();

  readFluxgateCAN();

  readAccelerometer();
  readTemperature();

}

bool sampleReady() {
  if (!sampleTickPending) {
    return false;
  }
  unsigned long now = millis();
  if ((long)(now - sampleReadyAtMs) < 0) {
    return false;
  }
  sampleTickPending = false;
  return true;
}

Sample buildSample() {
  Sample s;
  bool usedGnss = false;
  uint64_t epochMs = 0;
  if (pendingSampleEpochMs != 0) {
    epochMs = pendingSampleEpochMs;
    usedGnss = true;
  } else {
    epochMs = getEpochMs(usedGnss);
  }
  s.epochMs = epochMs;
  s.timeIsGnss = usedGnss;

  if (aggregator.countVoltage > 0) {
    s.hvVoltageV = aggregator.sumVoltage / aggregator.countVoltage;
  } else {
    s.hvVoltageV = NAN;
  }

  float currentAds = NAN;
  if (aggregator.countCurrentAds > 0) {
    currentAds = (float)(aggregator.sumCurrentAds / aggregator.countCurrentAds);
  }
  float currentFlux = NAN;
  if (aggregator.countCurrentFlux > 0) {
    currentFlux = (float)(aggregator.sumCurrentFlux / aggregator.countCurrentFlux);
  }
  switch (currentMode) {
    case MODE_ADS_ONLY:
      s.packCurrentA = currentAds;
      s.currentSrc = MODE_ADS_ONLY;
      break;
    case MODE_FLUX_ONLY:
      s.packCurrentA = currentFlux;
      s.currentSrc = MODE_FLUX_ONLY;
      break;
    case MODE_BOTH:

      s.packCurrentA = isnan(currentFlux) ? currentAds : currentFlux;
      s.currentSrc = isnan(currentFlux) ? MODE_ADS_ONLY : MODE_FLUX_ONLY;
      break;
    case MODE_AUTO:
    default:

      s.packCurrentA = isnan(currentFlux) ? currentAds : currentFlux;
      s.currentSrc = isnan(currentFlux) ? MODE_ADS_ONLY : MODE_FLUX_ONLY;
      break;
  }

  if (aggregator.countAccel > 0) {
    s.accelX = aggregator.sumAccelX / aggregator.countAccel;
    s.accelY = aggregator.sumAccelY / aggregator.countAccel;
    s.accelZ = aggregator.sumAccelZ / aggregator.countAccel;
  } else {
    s.accelX = s.accelY = s.accelZ = NAN;
  }

  if (aggregator.countTemp > 0) {
    s.tempC = aggregator.sumTemp / aggregator.countTemp;
  } else {
    s.tempC = NAN;
  }

  s.gnssValid = aggregator.gnssValid;
  s.lat = aggregator.gnssLat;
  s.lon = aggregator.gnssLon;
  s.alt = aggregator.gnssAlt;
  s.speed = aggregator.gnssSpeed;
  s.heading = (isfinite(aggregator.gnssSpeed) && aggregator.gnssSpeed > MIN_HEADING_SPEED_MS) ? aggregator.gnssHeading : NAN;
  s.hdop = aggregator.gnssHdop;
  s.sats = aggregator.gnssSats;

  s.vehCanCount = aggregator.vehCanCount;
  s.fluxCanCount = aggregator.fluxCanCount;

  aggregator.gnssValid = false;
  gnssFixAvailable = false;

  return s;
}

bool validateSample(const Sample &s) {

  if (s.epochMs == 0) return false;
  if (isnan(s.hvVoltageV) && isnan(s.packCurrentA)) return false;

  return true;
}

void storeSample(const Sample &s, int lteOk) {
  if (!sdReady) return;

  time_t t = (time_t)(s.epochMs / 1000ULL);
  struct tm tm;
  gmtime_r(&t, &tm);
  char fname[32];
  sprintf(fname, "/log_%04d%02d%02d.csv", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);

  if (!SD.exists(fname)) {
    File f = SD.open(fname, FILE_WRITE);
    if (f) {
      f.println("epoch_ms,gnss,voltage_V,current_A,src,ax,ay,az,temp,lat,lon,alt,speed,heading,hdop,sats,vehCAN,fluxCAN,lte_ok");
      f.close();
    }
  }
  File file = SD.open(fname, FILE_APPEND);
  if (file) {
    auto csvNum = [&](double v, int decimals){ if (isfinite(v)) file.print(v, decimals); else file.print("null"); };
    auto csvIntMaybe = [&](int v, bool valid){ if (valid) file.print(v); else file.print("null"); };
    file.print(s.epochMs);
    file.print(',');
    file.print(s.timeIsGnss ? 1 : 0);
    file.print(',');
    csvNum(s.hvVoltageV, 3);
    file.print(',');
    csvNum(s.packCurrentA, 3);
    file.print(',');
    file.print((int)s.currentSrc);
    file.print(',');
    csvNum(s.accelX, 2);
    file.print(',');
    csvNum(s.accelY, 2);
    file.print(',');
    csvNum(s.accelZ, 2);
    file.print(',');
    csvNum(s.tempC, 1);
    file.print(',');
    csvNum(s.lat, 6);
    file.print(',');
    csvNum(s.lon, 6);
    file.print(',');
    csvNum(s.alt, 1);
    file.print(',');
    csvNum(s.speed, 3);
    file.print(',');
    csvNum(s.heading, 3);
    file.print(',');
    csvNum(s.hdop, 2);
    file.print(',');
    csvIntMaybe(s.sats, s.gnssValid);
    file.print(',');
    file.print(s.vehCanCount);
    file.print(',');
    file.print(s.fluxCanCount);
    file.print(',');
    file.print(lteOk);
    file.println();
    file.close();
    lastSdOk = true;
  } else {
    lastSdOk = false;
  }
}

void publishSample(const Sample &s) {

  JsonDocument doc;
  auto putNum = [&](const char* key, double v){ if (isfinite(v)) doc[key] = v; else doc[key] = nullptr; };
  auto putIntMaybe = [&](const char* key, int v, bool valid){ if (valid) doc[key] = v; else doc[key] = nullptr; };
  doc["deviceID"] = DEVICE_ID;
  doc["timestamp"] = s.epochMs;
  doc["src"] = (int)s.currentSrc;

  putNum("voltage_V", s.hvVoltageV);
  putNum("current_A", s.packCurrentA);
  if (isfinite(s.hvVoltageV) && isfinite(s.packCurrentA)) {
    doc["power_kw"] = (s.hvVoltageV * s.packCurrentA) / 1000.0;
  } else {
    doc["power_kw"] = nullptr;
  }

  putNum("accel_x", s.accelX); putNum("accel_y", s.accelY); putNum("accel_z", s.accelZ);
  putNum("temp_C", s.tempC);

  if (s.gnssValid) {
    putNum("lat", s.lat); putNum("lon", s.lon); putNum("alt", s.alt);
    putNum("speed_m_s", s.speed); putNum("heading_deg", s.heading); putNum("hdop", s.hdop);
    doc["gnss_valid"] = true; putIntMaybe("sats", s.sats, true);
  } else {
    doc["gnss_valid"] = false;
    doc["lat"] = nullptr; doc["lon"] = nullptr; doc["alt"] = nullptr;
    doc["speed_m_s"] = nullptr; doc["heading_deg"] = nullptr; doc["hdop"] = nullptr; doc["sats"] = nullptr;
  }

  doc["vehCAN"] = s.vehCanCount; doc["fluxCAN"] = s.fluxCanCount;

  doc["lte_ok"] = 1;

  String jsonStrOk; serializeJson(doc, jsonStrOk);

  doc["lte_ok"] = 0;
  String jsonStrFail; serializeJson(doc, jsonStrFail);

  String hexOk, hexFail;
  for (size_t i = 0; i < jsonStrOk.length(); ++i) { char b[3]; sprintf(b, "%02X", (uint8_t)jsonStrOk[i]); hexOk += b; }
  for (size_t i = 0; i < jsonStrFail.length(); ++i) { char b[3]; sprintf(b, "%02X", (uint8_t)jsonStrFail[i]); hexFail += b; }
  lastPayloadLen = jsonStrOk.length();

  if (!lteDetected) {
    if (debugTrace && !lteDetected) Serial.println("[MQTT] LTE not detected — backlogging current sample");
    if (debugTrace && pendingQIsFull()) Serial.println("[MQTT] Pending queue full — backlogging current sample");
    backlogAppend(hexFail, s.epochMs);
    storeSample(s, 0);
    lastMqttOk = false;
    lastMainAttemptValid = false;
    lastMainSuccessFlag = false;
    return;
  }

  bool queued = publishHexPayload(hexOk, hexFail, s);
  if (!queued) {

    backlogAppend(hexFail, s.epochMs);
    storeSample(s, 0);
    lastMqttOk = false;
    lastMainAttemptValid = false;
    lastMainSuccessFlag = false;
  } else {
    lastMqttOk = false;
  }

}

bool syncNeeded() {
  unsigned long now = millis();
  if (now < nextSyncCheckMs) return false;
  nextSyncCheckMs = now + SYNC_CHECK_INTERVAL_MS;
  if (!lteMqttReady) { if (flushVerbose && debugTrace) Serial.println("[FLUSH] Check: MQTT not ready"); return false; }
  bool exists = backlogExists();
  if (flushVerbose && debugTrace) Serial.printf("[FLUSH] Check: backlogExists=%d\n", exists ? 1 : 0);
  if (flushDebug && debugTrace) {
    Serial.printf("[FLUSHDBG] syncNeeded mqttReady=%d backlog=%d\n", lteMqttReady ? 1 : 0, exists ? 1 : 0);
  }
  return exists;
}

static bool backlogAppend(const String &hexPayload, uint64_t epochMs) {
  if (!sdReady) return false;
  time_t t = (time_t)(epochMs / 1000ULL); struct tm tmv; gmtime_r(&t, &tmv);
  char blfn[32]; sprintf(blfn, "/backlog_%04d%02d%02d.txt", tmv.tm_year + 1900, tmv.tm_mon + 1, tmv.tm_mday);
  File b = SD.open(blfn, FILE_APPEND);
  if (!b) { if (debugTrace) Serial.printf("[BACKLOG] OPEN FAIL: %s\n", blfn); return false; }
  b.println(hexPayload);
  b.flush();
  b.close();

  return true;
}

static bool backlogExists() {
  if (!sdReady) return false;

  uint16_t y; uint8_t mon, day, weekday, hour, minute, second;
  if (rtcGetDateTime(y, mon, day, weekday, hour, minute, second)) {
    char blfn[32]; sprintf(blfn, "/backlog_%04d%02d%02d.txt", y, mon, day);
    if (SD.exists(blfn)) { if (flushVerbose && debugTrace) Serial.printf("[FLUSH] backlogExists: today file present %s\n", blfn); return true; }
  }
  File root = SD.open("/");
  if (!root) return false;
  File entry;
  while ((entry = root.openNextFile())) {
    String name = entry.name(); entry.close();
    if (name.startsWith("/")) name = name.substring(1);
    if (name.startsWith("backlog_") && name.endsWith(".txt")) { if (flushVerbose && debugTrace) Serial.printf("[FLUSH] backlogExists: found %s\n", name.c_str()); root.close(); return true; }
  }
  root.close();
  if (flushVerbose && debugTrace) Serial.println("[FLUSH] backlogExists: none");
  return false;
}

static bool findOldestBacklogFile(char* outPath, size_t outLen) {
  if (!sdReady) return false;
  String best = "";
  File root = SD.open("/"); if (!root) return false;
  File entry;
  while ((entry = root.openNextFile())) {
    String name = entry.name(); entry.close();
    if (name.startsWith("/")) name = name.substring(1);
    if (name.startsWith("backlog_") && name.endsWith(".txt")) {
      if (best.length() == 0 || name < best) best = name;
    }
  }
  root.close();
  if (best.length() == 0) return false;
  String full = String("/") + best;
  full.toCharArray(outPath, outLen);
  return true;
}

static size_t flushBacklogMulti(size_t maxRecords, unsigned long budgetMs) {
  if (!sdReady || maxRecords == 0) { if (debugTrace) Serial.println("[FLUSH] Guard: sdReady/maxRecords false"); return 0; }

  const unsigned long start   = millis();
  const unsigned long deadline = start + budgetMs;
  if (flushDebug && debugTrace) {
    Serial.printf("[FLUSHDBG] flushBacklogMulti start=%lu budget=%lu\n", start, budgetMs);
  }
  size_t sentCount = 0;
  size_t triedCount = 0;
  size_t keptCount  = 0;
  size_t pendingCount = 0;
  size_t failCount    = 0;
  size_t lineCount  = 0;

  for (size_t i=0; i<MAX_PENDING_SENDS && (long)(deadline - millis()) > 0; i++) {
    if (!backlogDone[i].active) continue;
    char path[40]; strncpy(path, backlogDone[i].path, sizeof(path)); path[sizeof(path)-1]='\0';
    File in = SD.open(path, FILE_READ);
    if (!in) { backlogDone[i].active=false; continue; }
    in.setTimeout(50);
    SD.remove("/backlog_tmp.txt");
    File temp = SD.open("/backlog_tmp.txt", FILE_WRITE);
    if (!temp) { in.close(); break; }
    temp.setTimeout(50);
    bool removedOnce = false;
    while (in.available() && (long)(deadline - millis()) > 0) {
      String line = in.readStringUntil('\n'); line.trim(); if (!line.length()) continue;
      if (!removedOnce && line == backlogDone[i].line) { removedOnce = true; continue; }
      temp.println(line);
    }
    in.close(); temp.flush(); temp.close();
    SD.remove(path);
    SD.rename("/backlog_tmp.txt", path);
    File chk = SD.open(path, FILE_READ);
    if (chk) { if (chk.size() == 0) { chk.close(); SD.remove(path); if (debugTrace) Serial.printf("[FLUSH] Deleted empty file: %s\n", path); } else chk.close(); }
    backlogDone[i].active = false;
  }

  static unsigned long lastSendMs = 0;
  while (triedCount < maxRecords && (long)(deadline - millis()) > 0) {
    if (pendingQIsFull()) break;
    char path[40];
    if (!findOldestBacklogFile(path, sizeof(path))) { if (flushVerbose && debugTrace) Serial.println("[FLUSH] No backlog files found"); break; }
    if (flushVerbose && debugTrace) { Serial.printf("[FLUSH] Oldest file: %s\n", path); }
    File in = SD.open(path, FILE_READ);
    if (!in) break;
    in.setTimeout(50);

    while (in.available() && triedCount < maxRecords && (long)(deadline - millis()) > 0 && !pendingQIsFull()) {
      String line = in.readStringUntil('\n'); line.trim(); if (!line.length()) continue;
      lineCount++;
      if (flushDebug && debugTrace) {
        Serial.printf("[FLUSHDBG] try backlog line from %s\n", path);
      }

      unsigned long nowt = millis();
      if (nowt - lastSendMs < 30) {
        unsigned long wait = 30 - (nowt - lastSendMs);
        if ((long)(deadline - millis()) <= (long)wait) { break; }
        delay(wait);
      }
      bool ok = publishHexPayloadBacklogEnqueue(path, line);
      if (ok) {
        pendingCount++;
        triedCount++;
        lastSendMs = millis();
        if (flushDebug && debugTrace) {
          Serial.println("[FLUSHDBG] backlog line queued");
        }
      } else {
        keptCount++;
        if (flushDebug && debugTrace) {
          Serial.println("[FLUSHDBG] backlog enqueue failed, stopping file");
        }
        break;
      }
    }
    in.close();

    break;
  }

  flushRanRecent = true;
  flushSentRecent = sentCount;
  backlogEnqueueSinceSummary += pendingCount;
  flushPendingRecent = backlogEnqueueSinceSummary;
  flushFailedRecent = failCount;
  flushTimeRecent = (unsigned long)(millis() - start);

  if (flushVerbose && debugTrace) {
    Serial.printf("[FLUSH] Stats: lines=%u tried=%u sent=%u kept=%u time=%lums\n",
                  (unsigned)lineCount, (unsigned)triedCount, (unsigned)sentCount, (unsigned)keptCount,
                  (unsigned long)flushTimeRecent);
  }
  return sentCount;
}

void syncWithCloud() {
  if (!sdReady) { if (debugTrace) Serial.println("[FLUSH] SD not ready — skip"); return; }
  if (!lteMqttReady) { if (debugTrace) Serial.println("[FLUSH] MQTT not ready — skip"); return; }
  if (flushVerbose && debugTrace) {Serial.println("[FLUSH] Starting flush");}
  size_t sent = flushBacklogMulti(BACKLOG_FLUSH_MAX_RECORDS, BACKLOG_FLUSH_BUDGET_MS);
  if (flushVerbose && debugTrace) {
    Serial.printf("[FLUSH] Complete: sent=%u records\n", (unsigned)sent);
  }
}

#define MCP7940_ADDR   0x6F

static uint8_t decToBcd(uint8_t v){ return ((v/10)<<4) | (v%10); }
static uint8_t bcdToDec(uint8_t v){ return ((v>>4)*10) + (v & 0x0F); }

static bool rtcWriteReg(uint8_t reg, uint8_t value){
  rtcI2C.beginTransmission(MCP7940_ADDR); rtcI2C.write(reg); rtcI2C.write(value);
  return rtcI2C.endTransmission() == 0;
}
static bool rtcReadRegs(uint8_t reg, uint8_t* buf, size_t len){
  rtcI2C.beginTransmission(MCP7940_ADDR); rtcI2C.write(reg);
  if (rtcI2C.endTransmission(false) != 0) return false;
  size_t n = rtcI2C.requestFrom(MCP7940_ADDR, (uint8_t)len);
  if (n != len) return false; for (size_t i=0;i<len;i++) buf[i]=rtcI2C.read(); return true;
}

bool rtcStartOscillator(){
  uint8_t sec;
  if (!rtcReadRegs(0x00, &sec, 1)) return false;

  sec |= 0x80;
  if (!rtcWriteReg(0x00, sec)) return false;

  delay(10);
  if (!rtcReadRegs(0x00, &sec, 1)) return false;

  return (sec & 0x80) != 0;
}

bool rtcSetDateTime(uint16_t year, uint8_t month, uint8_t day, uint8_t weekday,
                    uint8_t hour, uint8_t minute, uint8_t second) {

  if (!rtcWriteReg(0x00, decToBcd(second) & 0x7F)) return false;
  if (!rtcWriteReg(0x01, decToBcd(minute))) return false;
  if (!rtcWriteReg(0x02, decToBcd(hour) & 0x3F)) return false;

  uint8_t wkreg = 0;
  (void)rtcReadRegs(0x03, &wkreg, 1);

  wkreg |= (1u<<3);

  uint8_t wk = weekday & 0x07; if (wk == 0) wk = 1;
  wkreg = (uint8_t)((wkreg & ~0x07u) | (wk & 0x07u));
  if (!rtcWriteReg(0x03, wkreg)) return false;

  if (!rtcWriteReg(0x04, decToBcd(day))) return false;
  if (!rtcWriteReg(0x05, decToBcd(month) & 0x1F)) return false;
  if (!rtcWriteReg(0x06, decToBcd(year % 100))) return false;

  return rtcStartOscillator();
}

void rtcPrintDateTime() {
  uint16_t y; uint8_t mo, d, wd, h, m, s;
  if (rtcGetDateTime(y, mo, d, wd, h, m, s)) {
    Serial.printf("[RTC] %04u-%02u-%02u %02u:%02u:%02u (weekday=%u)\n",
                  y, mo, d, h, m, s, wd);
  } else {
    Serial.println("[RTC] invalid/unreadable time");
  }
}

bool rtcGetDateTime(uint16_t &year,uint8_t &month,uint8_t &day,uint8_t &weekday,uint8_t &hour,uint8_t &minute,uint8_t &second){
  uint8_t b[7];
  if (!rtcReadRegs(0x00, b, 7)) return false;

  second = bcdToDec(b[0] & 0x7F);
  minute = bcdToDec(b[1] & 0x7F);
  hour = bcdToDec(b[2] & 0x3F);
  weekday = bcdToDec(b[3] & 0x07);
  day = bcdToDec(b[4] & 0x3F);
  month = bcdToDec(b[5] & 0x1F);

  uint8_t yr_low = bcdToDec(b[6]);
  year = 2000 + yr_low + ((b[5] & 0x80) ? 100 : 0);

  if (month < 1 || month > 12) return false;
  if (day < 1 || day > 31) return false;
  if (weekday < 1 || weekday > 7) return false;
  if (hour > 23 || minute > 59 || second > 59) return false;

  return true;
}

uint32_t rtcGetEpoch(){
  uint16_t year; uint8_t month, day, wd, hour, minute, second;
  if (!rtcGetDateTime(year, month, day, wd, hour, minute, second)) return 0;

  int Y = (int)year;
  int M = (int)month;
  int D = (int)day;
  int hh = (int)hour;
  int mm = (int)minute;
  int ss = (int)second;

  int y = Y - (M <= 2);
  int era = (y >= 0 ? y : y - 399) / 400;
  unsigned yoe = (unsigned)(y - era * 400);
  unsigned doy = (unsigned)((153 * (M + (M > 2 ? -3 : 9)) + 2) / 5 + D - 1);
  unsigned doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  int64_t days = (int64_t)era * 146097 + (int64_t)doe - 719468;

  return (uint32_t)(days * 86400LL + hh * 3600 + mm * 60 + ss);
}

void rtcSyncFromEpoch(time_t epoch){
  struct tm tmv; gmtime_r(&epoch, &tmv);
  uint16_t y = tmv.tm_year + 1900; uint8_t mon = tmv.tm_mon + 1; uint8_t d = tmv.tm_mday;
  uint8_t wd = tmv.tm_wday + 1; uint8_t h = tmv.tm_hour; uint8_t m = tmv.tm_min; uint8_t s = tmv.tm_sec;
  rtcSetDateTime(y, mon, d, wd, h, m, s);
}

uint64_t getEpochMs(bool &isGnss) {
  static uint64_t lastOut = 0;
  static unsigned long lastTickMs = 0;
  unsigned long nowMs = millis();
  if (lastTickMs == 0) lastTickMs = nowMs;

  isGnss = false;
  if (gnssTimeAvailable && (gnssFixDim >= 2 || gnssFixAvailable)) {
    uint64_t epochMs = gnssEpochMs + (uint64_t)(nowMs - gnssTimeAnchorMillis);
    isGnss = true;
    lastOut = epochMs;
    lastTickMs = nowMs;
    return epochMs;
  }

  uint32_t rtc = rtcGetEpoch();
  if (rtc > 0) {
    uint64_t epochMs = (uint64_t)rtc * 1000ULL;
    lastOut = epochMs;
    lastTickMs = nowMs;
    return epochMs;
  }

  unsigned long delta = nowMs - lastTickMs;
  uint64_t epochMs = lastOut ? lastOut + (uint64_t)delta : (uint64_t)nowMs;
  lastOut = epochMs;
  lastTickMs = nowMs;
  return epochMs;
}

static void rtcSyncOnceOnMqttUp() {
  static bool done = false;
  if (done || !lteMqttReady) return;
  if (gnssTimeAvailable) {
    rtcSyncFromEpoch((time_t)(gnssEpochMs / 1000ULL));
    rtcFirstSynced = true;
    if (timeVerbose) Serial.println("[TIME] RTC synced from GNSS (on MQTT up)");
  }
  done = true;
}

void handleRtcResync(unsigned long currentMs) {
  if (currentMs - lastRtcSyncMs < RTC_RESYNC_INTERVAL_MS) {
    return;
  }

  if (gnssFixAvailable && gnssTimeAvailable) {

    uint64_t gnssEpoch = gnssEpochMs;
    uint64_t rtcEpoch = (uint64_t)rtcGetEpoch() * 1000ULL;
    int64_t diff = (int64_t)gnssEpoch - (int64_t)rtcEpoch;
    if (llabs(diff) > 500) {
      rtcSyncFromEpoch((time_t)(gnssEpoch / 1000ULL));
      lastRtcSyncMs = currentMs;
      if (timeVerbose) Serial.println("[TIME] RTC resynced from GNSS");
    }
    return;
  }
}

float convertAdsCountsToVolts(int32_t code) {
  return code * ADS_LSB_V;
}

float convertFluxRawToCurrent(uint32_t raw) {

  int32_t signed_cnt = int32_t(raw) - 0x800000;
  return float(signed_cnt) * 0.001f;
}

float convertAdsCountsToCurrent(int32_t code) {
  float v = code * ADS_LSB_V;
  return v * CURRENT_A_PER_V;
}
