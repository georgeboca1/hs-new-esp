#include <Arduino.h>
#include <BLE2902.h>
#include <BLEDevice.h>
#include <BLEServer.h>
#include <Wire.h>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <math.h>


#include <Adafruit_INA219.h>
#include <Adafruit_SSD1306.h>
#include <ArduinoJson.h>
#include <DFRobot_BMI160.h>
#include <MAX30105.h>
#include <RTClib.h>
#include <heartRate.h>
#include <spo2_algorithm.h>

#include "freertos/FreeRTOS.h"
#include "freertos/task.h"

namespace {

constexpr uint8_t kPacketHeaderTelemetry = 0xAA;
constexpr uint8_t kPacketHeaderHealth = 0xAB;

constexpr uint8_t kI2cSdaPin = 5;
constexpr uint8_t kI2cSclPin = 6;
constexpr uint8_t kButton1Pin = 3;
constexpr uint8_t kButton2Pin = 2;
constexpr uint8_t kTempAdcPin = 1;

constexpr uint8_t kMax3010xAddress = 0x57;
constexpr uint8_t kIna219Address = 0x40;
constexpr uint8_t kRtcI2cAddress = 0x68;
constexpr uint8_t kBmi160Address = 0x69;

constexpr uint16_t kOledWidth = 128;
constexpr uint16_t kOledHeight = 64;
constexpr int8_t kOledResetPin = -1;
constexpr uint8_t kOledI2cAddress = 0x3C;

constexpr uint8_t kButtonNextPin = kButton1Pin;
constexpr uint8_t kButtonPrevPin = kButton2Pin;

constexpr uint32_t kSensorPeriodMs = 20;   // 50 Hz sensor loop
constexpr uint32_t kUiPeriodMs = 200;      // 5 Hz OLED update
constexpr uint32_t kPacketPeriodMs = 1000; // 1 Hz packet push
constexpr uint32_t kPhysioPeriodMs = 1000; // 1 Hz physiology sampling

constexpr float kParachuteAccelThresholdG = 3.5f;
constexpr uint32_t kParachuteAccelDurationMs = 1200;
constexpr float kExcessiveRotationThresholdDps = 180.0f;
constexpr uint32_t kExcessiveRotationDurationMs = 3000;
constexpr uint32_t kImmobilityDurationMs = 3000;
constexpr float kStressAlertThreshold = 75.0f;
constexpr float kBatteryCapacityMah = 550.0f;
constexpr float kUsbSupplyVoltageThresholdV = 4.45f;
constexpr float kNearZeroCurrentThresholdMa = 2.0f;
constexpr float kTempReferenceVoltageV = 3.3f;
constexpr uint32_t kPulseContactIrThreshold =
    20000; // Increased to ensure real finger contact
constexpr uint32_t kPulseContactIrDeltaThreshold = 600;
constexpr uint32_t kPulseContactHoldMs = 1500;
constexpr int32_t kPulseRecalcSamples = 25;
constexpr uint16_t kPulseSampleRateHz = 100;
constexpr uint32_t kPulseSamplePeriodMs = 1000u / kPulseSampleRateHz;
constexpr float kPulseHrStepLimitBpm = 18.0f;
constexpr float kPulseBeatOutlierToleranceBpm = 28.0f;

constexpr uint8_t kDisplayTabCount = 4;
constexpr int16_t kUiHeaderHeight = 12;
constexpr int16_t kUiFooterY = 55;
constexpr int16_t kUiRowStartY = 16;
constexpr int16_t kUiRowStepY = 10;
constexpr int16_t kUiValueX = 50;

constexpr uint16_t kFlagParachuteDeployed = (1u << 0);
constexpr uint16_t kFlagUncontrolledFall = (1u << 1);
constexpr uint16_t kFlagExcessiveRotation = (1u << 2);
constexpr uint16_t kFlagImmobility = (1u << 3);
constexpr uint16_t kFlagAbnormalHeartRate = (1u << 4);
constexpr uint16_t kFlagHighStress = (1u << 5);
constexpr uint16_t kFlagAccidentRisk = (1u << 6);
constexpr uint16_t kFlagAbnormalBehavior = (1u << 7);
constexpr uint16_t kFlagLowBattery = (1u << 8);

const char *kBleServiceUuid = "f8e2f200-bf43-4ccf-a52b-5f9d9cd10001";
const char *kBleTelemetryUuid = "f8e2f201-bf43-4ccf-a52b-5f9d9cd10001";
const char *kBleHealthUuid = "f8e2f202-bf43-4ccf-a52b-5f9d9cd10001";
const char *kBleDebugJsonUuid = "f8e2f203-bf43-4ccf-a52b-5f9d9cd10001";

enum class FlightState : uint8_t {
  IN_PLANE = 0,
  FREEFALL = 1,
  DEPLOYMENT = 2,
  CANOPY_DESCENT = 3,
};

enum class PositionState : uint8_t {
  UNKNOWN = 0,
  HORIZONTAL = 1,
  VERTICAL = 2,
  TRACKING = 3,
  TUMBLING = 4,
};

enum class PowerState : uint8_t {
  DISCHARGING = 0,
  CHARGING = 1,
};

struct Vec3 {
  float x;
  float y;
  float z;
};

struct Orientation {
  float pitchDeg;
  float rollDeg;
  float yawDeg;
};

template <size_t N> class SlidingWindow {
public:
  void add(float value) {
    buffer_[writeIndex_] = value;
    writeIndex_ = (writeIndex_ + 1) % N;
    if (count_ < N) {
      ++count_;
    }
  }

  float mean() const {
    if (count_ == 0) {
      return 0.0f;
    }
    float sum = 0.0f;
    for (size_t i = 0; i < count_; ++i) {
      sum += buffer_[i];
    }
    return sum / static_cast<float>(count_);
  }

  float variance() const {
    if (count_ < 2) {
      return 0.0f;
    }
    const float m = mean();
    float sum = 0.0f;
    for (size_t i = 0; i < count_; ++i) {
      const float d = buffer_[i] - m;
      sum += d * d;
    }
    return sum / static_cast<float>(count_ - 1);
  }

  size_t size() const { return count_; }

  void clear() {
    memset(buffer_, 0, sizeof(buffer_));
    writeIndex_ = 0;
    count_ = 0;
  }

private:
  float buffer_[N] = {0.0f};
  size_t writeIndex_ = 0;
  size_t count_ = 0;
};

#pragma pack(push, 1)
struct PacketAA {
  uint8_t header;
  uint32_t uptimeMs;
  uint8_t state;
  uint8_t position;
  float bodyTemperatureC;
  float stressPct;
  float spo2Pct;
  float heartRateBpm;
  float accelMagnitudeG;
  float gyroMagnitudeDps;
  float batteryPct;
  float riskScore;
  uint16_t flags;
  uint8_t checksum;
};

struct PacketAB {
  uint8_t header;
  uint32_t uptimeMs;
  float cpuLoadPct;
  float voltageV;
  float currentMa;
  float totalConsumedMah;
  float estBatteryLifeMin;
  uint8_t checksum;
};
#pragma pack(pop)

struct Telemetry {
  FlightState flightState = FlightState::IN_PLANE;
  PositionState positionState = PositionState::UNKNOWN;

  Vec3 accelG{0.0f, 0.0f, 1.0f};
  Vec3 gyroDps{0.0f, 0.0f, 0.0f};
  Orientation orientation{0.0f, 0.0f, 0.0f};

  float accelMagnitudeG = 1.0f;
  float gyroMagnitudeDps = 0.0f;
  float verticalSpeedMs = 0.0f; // Estimated

  float bodyTemperatureC = 36.5f;
  float externalTemperatureC = 20.0f; // NTC thermistor
  float bloodOxygenPct = 98.0f;
  float heartRateBpm = 82.0f;
  float stressPct = 20.0f;

  float voltageV = 3.95f;
  float currentMa = 120.0f;
  float totalConsumedMah = 0.0f;
  float batteryPct = 80.0f;
  float estimatedBatteryLifeMin = 220.0f;
  PowerState powerState = PowerState::DISCHARGING;

  float cpuLoadPct = 0.0f;
  float riskScore = 0.0f;
  uint16_t flags = 0;
};

struct Calibration {
  Vec3 accelBias{0.0f, 0.0f, 0.0f};
  Vec3 gyroBias{0.0f, 0.0f, 0.0f};
  bool calibrated = false;
};

struct Button {
  explicit Button(uint8_t inputPin)
      : pin(inputPin), lastRawPressed(false), stablePressed(false),
        lastDebounceMs(0) {}

  uint8_t pin;
  bool lastRawPressed;
  bool stablePressed;
  uint32_t lastDebounceMs;
};

Adafruit_SSD1306 g_display(kOledWidth, kOledHeight, &Wire, kOledResetPin);
Adafruit_INA219 g_ina219(kIna219Address);
RTC_DS3231 g_rtc;
MAX30105 g_pulseSensor;
DFRobot_BMI160 *g_bmi160 = nullptr;

BLECharacteristic *g_telemetryCharacteristic = nullptr;
BLECharacteristic *g_healthCharacteristic = nullptr;
BLECharacteristic *g_debugJsonCharacteristic = nullptr;

Telemetry g_telemetry;
Calibration g_calibration;
Button g_nextButton(kButtonNextPin);
Button g_prevButton(kButtonPrevPin);
uint8_t g_activeTab = 0;
bool g_bleClientConnected = false;

SlidingWindow<100> g_accelMagnitudeWindow;
SlidingWindow<100> g_gyroMagnitudeWindow;
SlidingWindow<8> g_pulseBeatBpmWindow;

uint32_t g_lastSensorMs = 0;
uint32_t g_lastUiMs = 0;
uint32_t g_lastPacketMs = 0;
uint32_t g_lastCpuSampleMs = 0;
uint32_t g_lastPhysioMs = 0;

uint32_t g_freefallCandidateStartMs = 0;
uint32_t g_openingShockStartMs = 0;
uint32_t g_deploymentMs = 0;
uint32_t g_excessiveRotationStartMs = 0;
uint32_t g_immobilityStartMs = 0;
uint32_t g_lastNormalGravityMs = 0;

bool g_imuReady = false;
bool g_inaReady = false;
bool g_rtcReady = false;
bool g_pulseReady = false;
bool g_biometricContactDetected = false;
bool g_pulseContactDetected = false;
bool g_pulseAlgorithmStabilized = false;

uint32_t g_lastPulseContactMs = 0;
uint32_t g_lastBeatMs = 0;
uint32_t g_pulseAmbientIr = 0;
uint32_t g_lastPulseIrSample = 0;
uint32_t g_lastPulseContactThresholdIr = kPulseContactIrThreshold;
float g_pulseHeartRateBpm = NAN;
float g_pulseSpo2Pct = NAN;
uint32_t g_pulseIrBuffer[BUFFER_SIZE] = {0};
uint32_t g_pulseRedBuffer[BUFFER_SIZE] = {0};
int32_t g_pulseBufferCount = 0;
int32_t g_pulseSamplesSinceCalc = 0;

float g_prevAccelMagLp = 1.0f;
float g_prevGyroMagLp = 0.0f;

bool g_runtimeStatsInitialized = false;
uint32_t g_lastRunTimeTotal = 0;
uint32_t g_lastRunTimeIdle = 0;

class BleServerCallbacks final : public BLEServerCallbacks {
  void onConnect(BLEServer *server) override {
    (void)server;
    g_bleClientConnected = true;
  }

  void onDisconnect(BLEServer *server) override {
    g_bleClientConnected = false;
    server->getAdvertising()->start();
  }
};

BleServerCallbacks g_serverCallbacks;

float magnitude(const Vec3 &v) {
  return sqrtf((v.x * v.x) + (v.y * v.y) + (v.z * v.z));
}

float lowPass(float previous, float current, float alpha) {
  return (alpha * current) + ((1.0f - alpha) * previous);
}

float smoothHeartRateEstimate(float previousBpm, float candidateBpm,
                              float alpha) {
  if (!isfinite(candidateBpm)) {
    return previousBpm;
  }

  if (!isfinite(previousBpm)) {
    return candidateBpm;
  }

  const float bounded =
      constrain(candidateBpm, previousBpm - kPulseHrStepLimitBpm,
                previousBpm + kPulseHrStepLimitBpm);
  return lowPass(previousBpm, bounded, alpha);
}

uint8_t computeChecksum(const uint8_t *bytes, size_t lengthWithoutChecksum) {
  uint8_t checksum = 0;
  for (size_t i = 0; i < lengthWithoutChecksum; ++i) {
    checksum ^= bytes[i];
  }
  return checksum;
}

bool updateButton(Button &button, uint32_t nowMs) {
  const bool rawPressed = digitalRead(button.pin) == LOW;
  bool pressedEvent = false;

  if (rawPressed != button.lastRawPressed) {
    button.lastDebounceMs = nowMs;
    button.lastRawPressed = rawPressed;
  }

  if ((nowMs - button.lastDebounceMs) > 35 &&
      rawPressed != button.stablePressed) {
    button.stablePressed = rawPressed;
    if (button.stablePressed) {
      pressedEvent = true;
    }
  }

  return pressedEvent;
}

const char *toString(FlightState state) {
  switch (state) {
  case FlightState::IN_PLANE:
    return "IN_PLANE";
  case FlightState::FREEFALL:
    return "FREEFALL";
  case FlightState::DEPLOYMENT:
    return "DEPLOYMENT";
  case FlightState::CANOPY_DESCENT:
    return "CANOPY";
  default:
    return "UNKNOWN";
  }
}

const char *toString(PositionState state) {
  switch (state) {
  case PositionState::UNKNOWN:
    return "UNKNOWN";
  case PositionState::HORIZONTAL:
    return "HORIZONTAL";
  case PositionState::VERTICAL:
    return "VERTICAL";
  case PositionState::TRACKING:
    return "TRACKING";
  case PositionState::TUMBLING:
    return "TUMBLING";
  default:
    return "UNKNOWN";
  }
}

const char *toString(PowerState state) {
  switch (state) {
  case PowerState::DISCHARGING:
    return "DISCHRG";
  case PowerState::CHARGING:
    return "CHRG";
  default:
    return "UNKNOWN";
  }
}

void drawUiChrome(const char *title) {
  g_display.fillRect(0, 0, kOledWidth, kUiHeaderHeight, SSD1306_WHITE);
  g_display.setTextColor(SSD1306_BLACK);
  g_display.setCursor(3, 2);
  g_display.print(title);
  g_display.setCursor(82, 2);
  g_display.print(g_bleClientConnected ? "BLE ON" : "BLE OFF");
  g_display.setTextColor(SSD1306_WHITE);

  g_display.drawFastHLine(0, kUiHeaderHeight, kOledWidth, SSD1306_WHITE);
  g_display.drawFastHLine(0, kUiFooterY - 1, kOledWidth, SSD1306_WHITE);

  const int16_t tabW = 22;
  const int16_t tabH = 7;
  const int16_t startX = 12;
  const int16_t spacing = 29;
  const int16_t tabY = kUiFooterY + 1;

  for (uint8_t i = 0; i < kDisplayTabCount; ++i) {
    const int16_t x = startX + (static_cast<int16_t>(i) * spacing);
    if (i == g_activeTab) {
      g_display.fillRoundRect(x, tabY, tabW, tabH, 2, SSD1306_WHITE);
      g_display.setTextColor(SSD1306_BLACK);
    } else {
      g_display.drawRoundRect(x, tabY, tabW, tabH, 2, SSD1306_WHITE);
      g_display.setTextColor(SSD1306_WHITE);
    }

    g_display.setCursor(x + 8, tabY);
    g_display.write(static_cast<uint8_t>('1' + i));
  }

  g_display.setTextColor(SSD1306_WHITE);
}

void drawMetricRow(uint8_t rowIndex, const char *label, const char *value) {
  const int16_t y =
      kUiRowStartY + (static_cast<int16_t>(rowIndex) * kUiRowStepY);
  g_display.setCursor(2, y);
  g_display.print(label);
  g_display.setCursor(kUiValueX, y);
  g_display.print(value);

  if (rowIndex < 3) {
    g_display.drawFastHLine(1, y + 8, kOledWidth - 2, SSD1306_WHITE);
  }
}

void drawTab1() {
  char value[24];

  snprintf(value, sizeof(value), "%.2f C", g_telemetry.bodyTemperatureC);
  drawMetricRow(0, "TEMP", value);

  if (!g_pulseReady) {
    drawMetricRow(1, "SPO2", "NO SENSOR");
    drawMetricRow(2, "HEART", "NO SENSOR");
    drawMetricRow(3, "STRESS", "--");
    return;
  }

  if (!g_biometricContactDetected) {
    drawMetricRow(1, "SPO2", "--");
    drawMetricRow(2, "HEART", "NO TOUCH");
    drawMetricRow(3, "STRESS", "--");
    return;
  }

  snprintf(value, sizeof(value), "%.1f %%", g_telemetry.bloodOxygenPct);
  drawMetricRow(1, "SPO2", value);
  snprintf(value, sizeof(value), "%.1f bpm", g_telemetry.heartRateBpm);
  drawMetricRow(2, "HEART", value);
  snprintf(value, sizeof(value), "%.1f %%", g_telemetry.stressPct);
  drawMetricRow(3, "STRESS", value);
}

void drawTab2() {
  char value[24];

  drawMetricRow(0, "STATE", toString(g_telemetry.flightState));
  drawMetricRow(1, "POS", toString(g_telemetry.positionState));
  snprintf(value, sizeof(value), "%.2f g", g_telemetry.accelMagnitudeG);
  drawMetricRow(2, "|A|", value);
  snprintf(value, sizeof(value), "%.1f dps", g_telemetry.gyroMagnitudeDps);
  drawMetricRow(3, "|W|", value);
}

void drawTab3() {
  char value[24];

  snprintf(value, sizeof(value), "%.1f %%", g_telemetry.cpuLoadPct);
  drawMetricRow(0, "CPU", value);
  snprintf(value, sizeof(value), "%.0f %% %s", g_telemetry.batteryPct,
           toString(g_telemetry.powerState));
  drawMetricRow(1, "BAT", value);
  snprintf(value, sizeof(value), "%.2fV %+.0fmA", g_telemetry.voltageV,
           g_telemetry.currentMa);
  drawMetricRow(2, "V/I", value);

  if (g_telemetry.powerState == PowerState::CHARGING) {
    drawMetricRow(3, "LIFE", "CHARGING");
  } else {
    snprintf(value, sizeof(value), "%.0f min",
             g_telemetry.estimatedBatteryLifeMin);
    drawMetricRow(3, "LIFE", value);
  }
}

void drawTab4() {
  char value[24];

  snprintf(value, sizeof(value), "0x%04X", g_telemetry.flags);
  drawMetricRow(0, "FLAGS", value);
  snprintf(value, sizeof(value), "%.1f %%", g_telemetry.riskScore);
  drawMetricRow(1, "RISK", value);
  drawMetricRow(2, "BLE", g_bleClientConnected ? "CONNECTED" : "WAITING");
  drawMetricRow(3, "PKT", "AA/AB @ 1Hz");
}

void updateDisplay() {
  static const char *kTabTitles[kDisplayTabCount] = {"PHYSIO", "FLIGHT",
                                                     "POWER", "FLAGS"};

  g_display.clearDisplay();
  g_display.setTextColor(SSD1306_WHITE);
  g_display.setTextSize(1);
  drawUiChrome(kTabTitles[g_activeTab]);

  switch (g_activeTab) {
  case 0:
    drawTab1();
    break;
  case 1:
    drawTab2();
    break;
  case 2:
    drawTab3();
    break;
  case 3:
    drawTab4();
    break;
  default:
    drawTab1();
    break;
  }

  g_display.display();
}

void updateOrientation(float dtSeconds) {
  const float ax = g_telemetry.accelG.x;
  const float ay = g_telemetry.accelG.y;
  const float az = g_telemetry.accelG.z;

  const float gx = g_telemetry.gyroDps.x;
  const float gy = g_telemetry.gyroDps.y;
  const float gz = g_telemetry.gyroDps.z;

  const float pitchAcc =
      atan2f(-ax, sqrtf((ay * ay) + (az * az))) * (180.0f / PI);
  const float rollAcc = atan2f(ay, az) * (180.0f / PI);

  // Lightweight fusion: gyro integrates fast motion, accel corrects gravity
  // drift.
  g_telemetry.orientation.pitchDeg =
      (0.98f * (g_telemetry.orientation.pitchDeg + (gy * dtSeconds))) +
      (0.02f * pitchAcc);
  g_telemetry.orientation.rollDeg =
      (0.98f * (g_telemetry.orientation.rollDeg + (gx * dtSeconds))) +
      (0.02f * rollAcc);
  g_telemetry.orientation.yawDeg += gz * dtSeconds;

  if (g_telemetry.orientation.yawDeg > 180.0f) {
    g_telemetry.orientation.yawDeg -= 360.0f;
  } else if (g_telemetry.orientation.yawDeg < -180.0f) {
    g_telemetry.orientation.yawDeg += 360.0f;
  }
}

void updateDerivedMetrics(float dtSeconds) {
  g_telemetry.accelMagnitudeG = magnitude(g_telemetry.accelG);
  g_telemetry.gyroMagnitudeDps = magnitude(g_telemetry.gyroDps);

  g_prevAccelMagLp =
      lowPass(g_prevAccelMagLp, g_telemetry.accelMagnitudeG, 0.25f);
  g_prevGyroMagLp =
      lowPass(g_prevGyroMagLp, g_telemetry.gyroMagnitudeDps, 0.2f);

  g_accelMagnitudeWindow.add(g_prevAccelMagLp);
  g_gyroMagnitudeWindow.add(g_prevGyroMagLp);

  const float normalizedMotion =
      constrain(g_gyroMagnitudeWindow.variance() / 3500.0f, 0.0f, 1.0f);
  if (g_biometricContactDetected) {
    const float normalizedHr =
        constrain((g_telemetry.heartRateBpm - 55.0f) / 95.0f, 0.0f, 1.0f);
    g_telemetry.stressPct =
        constrain((0.65f * normalizedHr + 0.35f * normalizedMotion) * 100.0f,
                  0.0f, 100.0f);
  } else {
    g_telemetry.stressPct = 0.0f;
  }

  const bool usbLikeVoltage =
      g_telemetry.voltageV >= kUsbSupplyVoltageThresholdV;
  const bool nearZeroCurrent =
      fabsf(g_telemetry.currentMa) <= kNearZeroCurrentThresholdMa;
  const bool chargingCurrent =
      g_telemetry.currentMa < -kNearZeroCurrentThresholdMa;
  g_telemetry.powerState =
      ((usbLikeVoltage && nearZeroCurrent) || chargingCurrent)
          ? PowerState::CHARGING
          : PowerState::DISCHARGING;

  if (g_telemetry.powerState == PowerState::DISCHARGING &&
      g_telemetry.currentMa > kNearZeroCurrentThresholdMa) {
    g_telemetry.totalConsumedMah +=
        g_telemetry.currentMa * (dtSeconds / 3600.0f);
  }

  const float batterySenseVoltage = constrain(g_telemetry.voltageV, 3.2f, 4.2f);
  g_telemetry.batteryPct = constrain(
      ((batterySenseVoltage - 3.2f) / (4.2f - 3.2f)) * 100.0f, 0.0f, 100.0f);

  const float remainingMah =
      kBatteryCapacityMah * (g_telemetry.batteryPct / 100.0f);
  if (g_telemetry.powerState == PowerState::DISCHARGING &&
      g_telemetry.currentMa > kNearZeroCurrentThresholdMa) {
    g_telemetry.estimatedBatteryLifeMin =
        (remainingMah / g_telemetry.currentMa) * 60.0f;
  } else {
    g_telemetry.estimatedBatteryLifeMin = -1.0f;
  }

  // Vertical Speed Estimation (Simplified for skydive context)
  if (g_telemetry.flightState == FlightState::FREEFALL) {
    // In freefall, we accelerate towards terminal velocity (~55 m/s)
    // We use the accelerometer to detect if we are tracking or belly flying
    float targetSpeed =
        (g_telemetry.positionState == PositionState::VERTICAL) ? 75.0f : 54.0f;
    g_telemetry.verticalSpeedMs =
        lowPass(g_telemetry.verticalSpeedMs, targetSpeed, 0.05f);
  } else if (g_telemetry.flightState == FlightState::CANOPY_DESCENT) {
    g_telemetry.verticalSpeedMs =
        lowPass(g_telemetry.verticalSpeedMs, 5.0f, 0.1f);
  } else if (g_telemetry.flightState == FlightState::DEPLOYMENT) {
    // Rapid deceleration during opening shock
    g_telemetry.verticalSpeedMs =
        lowPass(g_telemetry.verticalSpeedMs, 10.0f, 0.3f);
  } else {
    g_telemetry.verticalSpeedMs =
        lowPass(g_telemetry.verticalSpeedMs, 0.0f, 0.2f);
  }
}

void updateFlightAndRisk(uint32_t nowMs) {
  uint16_t flags = 0;

  const float accelVar = g_accelMagnitudeWindow.variance();
  const float gyroVar = g_gyroMagnitudeWindow.variance();

  // Freefall can only trigger after a valid 1g-like phase was seen at least
  // once.
  if (g_prevAccelMagLp > 0.75f && g_prevAccelMagLp < 1.35f) {
    g_lastNormalGravityMs = nowMs;
  }
  const bool freefallArmed = g_lastNormalGravityMs != 0;

  if (g_telemetry.flightState == FlightState::IN_PLANE) {
    if (freefallArmed &&
        g_prevAccelMagLp < 0.45f) { // Increased threshold for easier simulation
      if (g_freefallCandidateStartMs == 0) {
        g_freefallCandidateStartMs = nowMs;
      }
      if ((nowMs - g_freefallCandidateStartMs) >
          200) { // Reduced duration for faster response
        g_telemetry.flightState = FlightState::FREEFALL;
      }
    } else {
      g_freefallCandidateStartMs = 0;
    }
  }

  if (g_telemetry.flightState == FlightState::FREEFALL) {
    if (g_prevAccelMagLp > kParachuteAccelThresholdG) {
      if (g_openingShockStartMs == 0) {
        g_openingShockStartMs = nowMs;
      }
      if ((nowMs - g_openingShockStartMs) >= kParachuteAccelDurationMs) {
        g_telemetry.flightState = FlightState::DEPLOYMENT;
        g_deploymentMs = nowMs;
        flags |= kFlagParachuteDeployed;
      }
    } else {
      g_openingShockStartMs = 0;
    }
  }

  if (g_telemetry.flightState == FlightState::DEPLOYMENT) {
    flags |= kFlagParachuteDeployed;
    if ((nowMs - g_deploymentMs) > 3000 && g_prevAccelMagLp > 0.7f &&
        g_prevAccelMagLp < 1.6f) {
      g_telemetry.flightState = FlightState::CANOPY_DESCENT;
    }
  }

  if (g_telemetry.flightState == FlightState::CANOPY_DESCENT) {
    flags |= kFlagParachuteDeployed;
  }

  if (g_prevGyroMagLp > kExcessiveRotationThresholdDps) {
    if (g_excessiveRotationStartMs == 0) {
      g_excessiveRotationStartMs = nowMs;
    }
    if ((nowMs - g_excessiveRotationStartMs) >= kExcessiveRotationDurationMs) {
      flags |= kFlagExcessiveRotation;
    }
  } else {
    g_excessiveRotationStartMs = 0;
  }

  if (accelVar > 0.35f && gyroVar > 2200.0f) {
    flags |= kFlagUncontrolledFall;
  }

  if (gyroVar < 12.0f && accelVar < 0.015f) {
    if (g_immobilityStartMs == 0) {
      g_immobilityStartMs = nowMs;
    }
    // Only flag immobility as a risk if we are actually in a flight phase
    if ((nowMs - g_immobilityStartMs) >= kImmobilityDurationMs &&
        g_telemetry.flightState != FlightState::IN_PLANE) {
      flags |= kFlagImmobility;
    }
  } else {
    g_immobilityStartMs = 0;
  }

  if (g_biometricContactDetected &&
      (g_telemetry.heartRateBpm < 45.0f || g_telemetry.heartRateBpm > 170.0f)) {
    flags |= kFlagAbnormalHeartRate;
  }

  if (g_biometricContactDetected &&
      g_telemetry.stressPct >= kStressAlertThreshold) {
    flags |= kFlagHighStress;
  }

  if (g_telemetry.batteryPct < 15.0f) {
    flags |= kFlagLowBattery;
  }

  if ((flags & kFlagUncontrolledFall) != 0) {
    g_telemetry.positionState = PositionState::TUMBLING;
  } else if (g_prevGyroMagLp > 65.0f) {
    g_telemetry.positionState = PositionState::TRACKING;
  } else {
    const float accelMagSafe = max(g_telemetry.accelMagnitudeG, 0.001f);
    const float zFraction = fabsf(g_telemetry.accelG.z) / accelMagSafe;
    if (zFraction > 0.82f) {
      g_telemetry.positionState = PositionState::HORIZONTAL;
    } else if (zFraction < 0.42f) {
      g_telemetry.positionState = PositionState::VERTICAL;
    } else {
      g_telemetry.positionState = PositionState::TRACKING;
    }
  }

  if (g_telemetry.flightState == FlightState::FREEFALL &&
      (((flags & kFlagUncontrolledFall) != 0) ||
       ((flags & kFlagExcessiveRotation) != 0))) {
    flags |= kFlagAbnormalBehavior;
  }

  float riskScore = 0.0f;
  if ((flags & kFlagUncontrolledFall) != 0) {
    riskScore += 35.0f;
  }
  if ((flags & kFlagExcessiveRotation) != 0) {
    riskScore += 25.0f;
  }
  if ((flags & kFlagImmobility) != 0) {
    riskScore += 25.0f;
  }
  if ((flags & kFlagAbnormalHeartRate) != 0) {
    riskScore += 10.0f;
  }
  if ((flags & kFlagHighStress) != 0) {
    riskScore += 8.0f;
  }
  if ((flags & kFlagLowBattery) != 0) {
    riskScore += 5.0f;
  }

  riskScore = constrain(riskScore, 0.0f, 100.0f);
  if (riskScore >= 60.0f) {
    flags |= kFlagAccidentRisk;
  }

  g_telemetry.riskScore = riskScore;
  g_telemetry.flags = flags;
}

float readBodyTemperatureC() {
  static float filteredTempC = 36.0f; // Default human temp baseline
  static bool initialized = false;

  float chosenTempC = NAN;
  bool validSample = false;

  // Read internal die temperature from MAX30102
  if (g_pulseReady) {
    const float maxTempC = g_pulseSensor.readTemperature();
    if (isfinite(maxTempC) && maxTempC > -40.0f && maxTempC < 85.0f) {
      chosenTempC = maxTempC;
      validSample = true;
    }
  }

  // Fallback to RTC temperature if pulse sensor fails (ambient board temp)
  if (!validSample && g_rtcReady) {
    const float rtcTempC = g_rtc.getTemperature();
    if (rtcTempC > -40.0f && rtcTempC < 85.0f) {
      chosenTempC = rtcTempC;
      validSample = true;
    }
  }

  if (!initialized) {
    filteredTempC = validSample ? chosenTempC : 36.0f;
    initialized = true;
    return filteredTempC;
  }

  if (validSample) {
    filteredTempC = lowPass(filteredTempC, chosenTempC, 0.18f);
  }

  return filteredTempC;
}

float readExternalTemperatureC() {
  uint16_t samples[100];
  for (int i = 0; i < 100; i++) {
    samples[i] = analogRead(kTempAdcPin);
    if (i % 25 == 0) {
      delay(1);
    }
  }
  std::sort(samples, samples + 100);
  uint16_t adcRaw = samples[50]; // Median

  // Relaxed range to capture data even at extremes
  if (adcRaw < 1 || adcRaw > 4094) {
    return NAN; // Pure 0 or 4095 is likely a hardware disconnect
  }

  // Calculate Resistance
  // NTC_GND_SIDE_DIVIDER = 0 (NTC is on 3.3V side and fixed resistor goes to
  // GND)
  const float fixedResistor = 10000.0f;
  float resistance = (fixedResistor * (4095.0f - static_cast<float>(adcRaw))) /
                     static_cast<float>(adcRaw);

  if (resistance < 10.0f || resistance > 1000000.0f) {
    return NAN;
  }

  // Steinhart-Hart equation
  const float nominalOhms = 10000.0f;
  const float betaValue = 3950.0f;
  const float refTempK = 25.0f + 273.15f;

  float steinhart =
      logf(resistance / nominalOhms) / betaValue + 1.0f / refTempK;
  float instantTemp = 1.0f / steinhart - 273.15f;

  // Temperature offset to compensate for component tolerances
  const float calibrationOffset = -5.7f;
  float calibrated = instantTemp + calibrationOffset;

  static float filteredExtTempC = -999.0f;

  // Fast Startup & Smooth EMA (Alpha = 0.40)
  if (filteredExtTempC < -900.0f) {
    if (instantTemp > -90.0f) {
      filteredExtTempC = calibrated;
    }
  } else {
    if (instantTemp > -90.0f) {
      filteredExtTempC = (filteredExtTempC * 0.60f) + (calibrated * 0.40f);
    }
  }

  return filteredExtTempC > -90.0f ? filteredExtTempC : NAN;
}

float readBloodOxygenPct() {
  if (!g_pulseReady || !g_pulseContactDetected || !isfinite(g_pulseSpo2Pct)) {
    return NAN;
  }
  return g_pulseSpo2Pct;
}

float readHeartRateBpm() {
  if (!g_pulseReady || !g_pulseContactDetected ||
      !isfinite(g_pulseHeartRateBpm)) {
    return NAN;
  }
  return g_pulseHeartRateBpm;
}

void pushPulseSample(uint32_t irSample, uint32_t redSample) {
  if (g_pulseBufferCount < BUFFER_SIZE) {
    g_pulseIrBuffer[g_pulseBufferCount] = irSample;
    g_pulseRedBuffer[g_pulseBufferCount] = redSample;
    ++g_pulseBufferCount;
  } else {
    memmove(g_pulseIrBuffer, g_pulseIrBuffer + 1,
            static_cast<size_t>(BUFFER_SIZE - 1) * sizeof(uint32_t));
    memmove(g_pulseRedBuffer, g_pulseRedBuffer + 1,
            static_cast<size_t>(BUFFER_SIZE - 1) * sizeof(uint32_t));
    g_pulseIrBuffer[BUFFER_SIZE - 1] = irSample;
    g_pulseRedBuffer[BUFFER_SIZE - 1] = redSample;
  }

  ++g_pulseSamplesSinceCalc;
  if (g_pulseBufferCount == BUFFER_SIZE &&
      g_pulseSamplesSinceCalc >= kPulseRecalcSamples) {
    int32_t spo2 = -999;
    int8_t spo2Valid = 0;
    int32_t heartRate = -999;
    int8_t heartRateValid = 0;
    maxim_heart_rate_and_oxygen_saturation(g_pulseIrBuffer, g_pulseBufferCount,
                                           g_pulseRedBuffer, &spo2, &spo2Valid,
                                           &heartRate, &heartRateValid);

    if (spo2Valid != 0 && spo2 >= 88 &&
        spo2 <= 100) { // Filter out unrealistic drops < 88%
      const float spo2f = static_cast<float>(spo2);
      g_pulseSpo2Pct = isfinite(g_pulseSpo2Pct)
                           ? lowPass(g_pulseSpo2Pct, spo2f, 0.22f)
                           : spo2f;

      // We got a valid SpO2 reading, so the algorithm has produced valid
      // results.
      g_pulseAlgorithmStabilized = true;
    }

    if (heartRateValid != 0 && heartRate >= 35 && heartRate <= 220) {
      const float hrf = static_cast<float>(heartRate);
      const bool outlierVsBeatTrend =
          (g_pulseBeatBpmWindow.size() >= 3) &&
          (fabsf(hrf - g_pulseBeatBpmWindow.mean()) >
           kPulseBeatOutlierToleranceBpm);
      if (!outlierVsBeatTrend) {
        g_pulseHeartRateBpm =
            smoothHeartRateEstimate(g_pulseHeartRateBpm, hrf, 0.2f);
      }
    }

    g_pulseSamplesSinceCalc = 0;
  }
}

void updatePulseReadings(uint32_t nowMs) {
  if (!g_pulseReady) {
    g_pulseContactDetected = false;
    return;
  }

  g_pulseSensor.check();
  int32_t pendingSamples = g_pulseSensor.available();
  while (pendingSamples > 0) {
    const int32_t sampleBackIndex = pendingSamples - 1;
    const uint32_t backtrackMs =
        static_cast<uint32_t>(sampleBackIndex) * kPulseSamplePeriodMs;
    const uint32_t sampleMs =
        (backtrackMs <= nowMs) ? (nowMs - backtrackMs) : nowMs;

    const uint32_t irSample = g_pulseSensor.getFIFOIR();
    const uint32_t redSample = g_pulseSensor.getFIFORed();
    g_pulseSensor.nextSample();
    --pendingSamples;

    if (irSample >= kPulseContactIrThreshold) {
      g_lastPulseContactMs = sampleMs;

      // Simple software low-pass filter to clean up noise for beat detection
      static float filteredIr = 0;
      filteredIr = lowPass(filteredIr, static_cast<float>(irSample), 0.1f);

      if (checkForBeat(static_cast<int32_t>(filteredIr))) {
        if (g_lastBeatMs != 0 && sampleMs > g_lastBeatMs) {
          const uint32_t deltaMs = sampleMs - g_lastBeatMs;
          if (deltaMs > 300 && deltaMs < 1500) { // Valid interval (40-200 BPM)
            const float bpm = 60000.0f / static_cast<float>(deltaMs);
            g_pulseBeatBpmWindow.add(bpm);
            g_pulseHeartRateBpm = smoothHeartRateEstimate(
                g_pulseHeartRateBpm, g_pulseBeatBpmWindow.mean(), 0.15f);
          }
        }
        g_lastBeatMs = sampleMs;
      }

      pushPulseSample(irSample, redSample);
    }
  }

  g_pulseContactDetected =
      (g_lastPulseContactMs != 0) &&
      ((nowMs - g_lastPulseContactMs) <= kPulseContactHoldMs);
  if (!g_pulseContactDetected) {
    g_lastBeatMs = 0;
    g_pulseBufferCount = 0;
    g_pulseSamplesSinceCalc = 0;
    g_pulseBeatBpmWindow.clear();
    g_pulseHeartRateBpm = NAN;
    g_pulseSpo2Pct = NAN;
  } else {
    // If we have a beat trend but algorithm hasn't finished, use trend
    if (isnan(g_pulseHeartRateBpm) && g_pulseBeatBpmWindow.size() >= 2) {
      g_pulseHeartRateBpm = g_pulseBeatBpmWindow.mean();
    }
  }
}

void updatePhysioReadings(uint32_t nowMs) {
  if ((nowMs - g_lastPhysioMs) < kPhysioPeriodMs) {
    return;
  }

  g_lastPhysioMs = nowMs;
  g_telemetry.bodyTemperatureC = readBodyTemperatureC();
  g_telemetry.externalTemperatureC = readExternalTemperatureC();

  const float spo2Candidate = readBloodOxygenPct();
  const float hrCandidate = readHeartRateBpm();

  g_biometricContactDetected = g_pulseReady && g_pulseContactDetected;

  if (g_biometricContactDetected) {
    if (isfinite(spo2Candidate)) {
      g_telemetry.bloodOxygenPct = constrain(spo2Candidate, 70.0f, 100.0f);
    }
    if (isfinite(hrCandidate)) {
      g_telemetry.heartRateBpm = constrain(hrCandidate, 35.0f, 220.0f);
    }
  } else {
    g_telemetry.bloodOxygenPct = 0.0f;
    g_telemetry.heartRateBpm = 0.0f;
  }
}

void updateImuReadings() {
  if (!g_imuReady || g_bmi160 == nullptr) {
    g_telemetry.accelG = {0.0f, 0.0f, 1.0f};
    g_telemetry.gyroDps = {0.0f, 0.0f, 0.0f};
    return;
  }

  int16_t rawData[6] = {0};
  static int i2cErrorCount = 0;

  if (g_bmi160->getAccelGyroData(rawData) != 0) {
    i2cErrorCount++;
    if (i2cErrorCount > 10) {
      Serial.println("IMU I2C FAIL - RECOVERY...");
      g_bmi160->softReset();
      g_bmi160->I2cInit(kBmi160Address);
      i2cErrorCount = 0;
    }
    return;
  }
  i2cErrorCount = 0;

  const int32_t rawActivity = abs(rawData[0]) + abs(rawData[1]) +
                              abs(rawData[2]) + abs(rawData[3]) +
                              abs(rawData[4]) + abs(rawData[5]);
  if (rawActivity == 0) {
    // Ignore clearly invalid all-zero frames from I2C glitches/sensor startup.
    return;
  }

  g_telemetry.accelG.x =
      (static_cast<float>(rawData[3]) / 16384.0f) - g_calibration.accelBias.x;
  g_telemetry.accelG.y =
      (static_cast<float>(rawData[4]) / 16384.0f) - g_calibration.accelBias.y;
  g_telemetry.accelG.z =
      (static_cast<float>(rawData[5]) / 16384.0f) - g_calibration.accelBias.z;

  g_telemetry.gyroDps.x =
      (static_cast<float>(rawData[0]) / 16.4f) - g_calibration.gyroBias.x;
  g_telemetry.gyroDps.y =
      (static_cast<float>(rawData[1]) / 16.4f) - g_calibration.gyroBias.y;
  g_telemetry.gyroDps.z =
      (static_cast<float>(rawData[2]) / 16.4f) - g_calibration.gyroBias.z;
}

void calibrateSensors() {
  if (!g_imuReady || g_bmi160 == nullptr)
    return;

  Serial.println("Calibrating IMU... Keep device flat and still.");
  g_display.clearDisplay();
  g_display.setCursor(0, 0);
  g_display.println("CALIBRATING IMU...");
  g_display.println("KEEP STILL");
  g_display.display();

  float ax = 0, ay = 0, az = 0;
  float gx = 0, gy = 0, gz = 0;
  int samples = 200;

  for (int i = 0; i < samples; i++) {
    int16_t raw[6];
    if (g_bmi160->getAccelGyroData(raw) == 0) {
      ax += static_cast<float>(raw[3]) / 16384.0f;
      ay += static_cast<float>(raw[4]) / 16384.0f;
      az += static_cast<float>(raw[5]) / 16384.0f;
      gx += static_cast<float>(raw[0]) / 16.4f;
      gy += static_cast<float>(raw[1]) / 16.4f;
      gz += static_cast<float>(raw[2]) / 16.4f;
    }
    delay(5);
  }

  g_calibration.accelBias.x = ax / samples;
  g_calibration.accelBias.y = ay / samples;
  g_calibration.accelBias.z = (az / samples) - 1.0f; // Assume Z is gravity
  g_calibration.gyroBias.x = gx / samples;
  g_calibration.gyroBias.y = gy / samples;
  g_calibration.gyroBias.z = gz / samples;
  g_calibration.calibrated = true;
  Serial.println("Calibration done.");
}

void updatePowerReadings() {
  if (!g_inaReady) {
    g_telemetry.currentMa = 0.0f;
    return;
  }

  g_telemetry.voltageV =
      g_ina219.getBusVoltage_V() + (g_ina219.getShuntVoltage_mV() / 1000.0f);
  g_telemetry.currentMa = g_ina219.getCurrent_mA();
}

float computeRuntimeStatsCpuLoad() {
#if (configUSE_TRACE_FACILITY == 1) && (configGENERATE_RUN_TIME_STATS == 1)
  const UBaseType_t taskCount = uxTaskGetNumberOfTasks();
  if (taskCount == 0) {
    return NAN;
  }

  TaskStatus_t *taskStatus = static_cast<TaskStatus_t *>(
      pvPortMalloc(taskCount * sizeof(TaskStatus_t)));
  if (taskStatus == nullptr) {
    return NAN;
  }

  uint32_t runTimeTotal = 0;
  const UBaseType_t filled =
      uxTaskGetSystemState(taskStatus, taskCount, &runTimeTotal);
  if (filled == 0 || runTimeTotal == 0) {
    vPortFree(taskStatus);
    return NAN;
  }

  uint32_t idleRunTime = 0;
  for (UBaseType_t i = 0; i < filled; ++i) {
    if (strncmp(taskStatus[i].pcTaskName, "IDLE", 4) == 0) {
      idleRunTime += taskStatus[i].ulRunTimeCounter;
    }
  }

  vPortFree(taskStatus);

  if (!g_runtimeStatsInitialized) {
    g_lastRunTimeTotal = runTimeTotal;
    g_lastRunTimeIdle = idleRunTime;
    g_runtimeStatsInitialized = true;
    return NAN;
  }

  const uint32_t deltaTotal = runTimeTotal - g_lastRunTimeTotal;
  const uint32_t deltaIdle = idleRunTime - g_lastRunTimeIdle;

  g_lastRunTimeTotal = runTimeTotal;
  g_lastRunTimeIdle = idleRunTime;

  if (deltaTotal == 0 || deltaIdle > deltaTotal) {
    return NAN;
  }

  const float idleRatio =
      static_cast<float>(deltaIdle) / static_cast<float>(deltaTotal);
  return constrain((1.0f - idleRatio) * 100.0f, 0.0f, 100.0f);
#else
  return NAN;
#endif
}

void updateCpuLoad(uint32_t nowMs) {
  if ((nowMs - g_lastCpuSampleMs) < 500) {
    return;
  }

  g_lastCpuSampleMs = nowMs;
  const float runtimeStatsLoad = computeRuntimeStatsCpuLoad();

  if (!isnan(runtimeStatsLoad)) {
    g_telemetry.cpuLoadPct = runtimeStatsLoad;
    return;
  }

  // Fallback estimate based on loop periodicity when runtime stats are
  // disabled.
  static uint32_t previousLoopMs = 0;
  if (previousLoopMs == 0) {
    previousLoopMs = nowMs;
    return;
  }

  const uint32_t elapsed = nowMs - previousLoopMs;
  previousLoopMs = nowMs;

  const uint32_t safeElapsed = (elapsed == 0) ? 1 : elapsed;
  const float duty = constrain(static_cast<float>(kSensorPeriodMs) /
                                   static_cast<float>(safeElapsed),
                               0.0f, 1.0f);
  g_telemetry.cpuLoadPct = duty * 100.0f;
}

void sendPackets(uint32_t nowMs) {
  PacketAA telemetryPacket{};
  telemetryPacket.header = kPacketHeaderTelemetry;
  telemetryPacket.uptimeMs = nowMs;
  telemetryPacket.state = static_cast<uint8_t>(g_telemetry.flightState);
  telemetryPacket.position = static_cast<uint8_t>(g_telemetry.positionState);
  telemetryPacket.bodyTemperatureC = g_telemetry.bodyTemperatureC;
  telemetryPacket.stressPct = g_telemetry.stressPct;
  telemetryPacket.spo2Pct = g_telemetry.bloodOxygenPct;
  telemetryPacket.heartRateBpm = g_telemetry.heartRateBpm;
  telemetryPacket.accelMagnitudeG = g_telemetry.accelMagnitudeG;
  telemetryPacket.gyroMagnitudeDps = g_telemetry.gyroMagnitudeDps;
  telemetryPacket.batteryPct = g_telemetry.batteryPct;
  telemetryPacket.riskScore = g_telemetry.riskScore;
  telemetryPacket.flags = g_telemetry.flags;
  telemetryPacket.checksum =
      computeChecksum(reinterpret_cast<const uint8_t *>(&telemetryPacket),
                      sizeof(PacketAA) - 1);

  PacketAB healthPacket{};
  healthPacket.header = kPacketHeaderHealth;
  healthPacket.uptimeMs = nowMs;
  healthPacket.cpuLoadPct = g_telemetry.cpuLoadPct;
  healthPacket.voltageV = g_telemetry.voltageV;
  healthPacket.currentMa = g_telemetry.currentMa;
  healthPacket.totalConsumedMah = g_telemetry.totalConsumedMah;
  healthPacket.estBatteryLifeMin = g_telemetry.estimatedBatteryLifeMin;
  healthPacket.checksum = computeChecksum(
      reinterpret_cast<const uint8_t *>(&healthPacket), sizeof(PacketAB) - 1);

  if (g_telemetryCharacteristic != nullptr) {
    g_telemetryCharacteristic->setValue(
        reinterpret_cast<uint8_t *>(&telemetryPacket), sizeof(PacketAA));
    if (g_bleClientConnected) {
      g_telemetryCharacteristic->notify();
    }
  }

  if (g_healthCharacteristic != nullptr) {
    g_healthCharacteristic->setValue(reinterpret_cast<uint8_t *>(&healthPacket),
                                     sizeof(PacketAB));
    if (g_bleClientConnected) {
      g_healthCharacteristic->notify();
    }
  }

  if (g_debugJsonCharacteristic != nullptr) {
    StaticJsonDocument<512> doc;
    doc["device_id"] = "PARA-01"; // Unique identifier for the skydiver

    // Add Unix Timestamp if RTC is running, otherwise use uptime
    if (g_rtcReady) {
      doc["timestamp"] = g_rtc.now().unixtime();
    } else {
      doc["timestamp"] = nowMs / 1000;
    }

    doc["state"] = toString(g_telemetry.flightState);
    doc["parachute"] =
        (g_telemetry.flags & kFlagParachuteDeployed) ? "DEPLOYED" : "STOWED";
    doc["body_position"] = toString(g_telemetry.positionState);
    doc["heart_rate"] = g_biometricContactDetected
                 ? static_cast<int>(g_telemetry.heartRateBpm)
                 : 0;
    doc["SpO2"] = (g_biometricContactDetected && g_pulseAlgorithmStabilized)
              ? g_telemetry.bloodOxygenPct
              : 0.0f;
    doc["temp"] = g_telemetry.bodyTemperatureC;
    doc["stress_level"] = g_telemetry.stressPct;
    doc["is_pulse_stable"] = g_pulseAlgorithmStabilized;
    doc["vertical_speed"] = g_telemetry.verticalSpeedMs;
    doc["rotation"] = g_telemetry.gyroMagnitudeDps;
    doc["g_force"] = g_telemetry.accelMagnitudeG;
    doc["temp_ext"] = isfinite(g_telemetry.externalTemperatureC)
                 ? g_telemetry.externalTemperatureC
                 : 0.0f;
    doc["battery_pct"] = g_telemetry.batteryPct;
    doc["risk_score"] = g_telemetry.riskScore;
    doc["alert_active"] =
      (g_telemetry.riskScore >= 25.0f); // Alert if risk is High

    char buffer[512];
    const size_t n = serializeJson(doc, buffer, sizeof(buffer));
    g_debugJsonCharacteristic->setValue(reinterpret_cast<uint8_t *>(buffer), n);
    if (g_bleClientConnected) {
      g_debugJsonCharacteristic->notify();
    }

    // Debug JSON to Serial
    Serial.print("JSON: ");
    serializeJson(doc, Serial);
    Serial.println();
  }

  Serial.printf("AA flags=0x%04X risk=%.1f state=%s pos=%s\n",
                g_telemetry.flags, g_telemetry.riskScore,
                toString(g_telemetry.flightState),
                toString(g_telemetry.positionState));
  Serial.printf("PHYS temp=%.2fC spo2=%.1f hr=%.1f contact=%s\n",
                g_telemetry.bodyTemperatureC, g_telemetry.bloodOxygenPct,
                g_telemetry.heartRateBpm,
                g_biometricContactDetected ? "YES" : "NO");
  if (g_telemetry.powerState == PowerState::CHARGING) {
    Serial.printf(
        "AB cpu=%.1f%% V=%.2f I=%.1f mA used=%.2f mAh life=CHARGING\n",
        g_telemetry.cpuLoadPct, g_telemetry.voltageV, g_telemetry.currentMa,
        g_telemetry.totalConsumedMah);
  } else {
    Serial.printf(
        "AB cpu=%.1f%% V=%.2f I=%.1f mA used=%.2f mAh life=%.0f min\n",
        g_telemetry.cpuLoadPct, g_telemetry.voltageV, g_telemetry.currentMa,
        g_telemetry.totalConsumedMah, g_telemetry.estimatedBatteryLifeMin);
  }
}

void setupBle() {
  BLEDevice::init("HS-ESP32C3-WEARABLE");
  BLEServer *server = BLEDevice::createServer();
  server->setCallbacks(&g_serverCallbacks);

  BLEService *service = server->createService(kBleServiceUuid);

  g_telemetryCharacteristic = service->createCharacteristic(
      kBleTelemetryUuid,
      BLECharacteristic::PROPERTY_READ | BLECharacteristic::PROPERTY_NOTIFY);
  g_healthCharacteristic = service->createCharacteristic(
      kBleHealthUuid,
      BLECharacteristic::PROPERTY_READ | BLECharacteristic::PROPERTY_NOTIFY);
  g_debugJsonCharacteristic = service->createCharacteristic(
      kBleDebugJsonUuid,
      BLECharacteristic::PROPERTY_READ | BLECharacteristic::PROPERTY_NOTIFY);

  g_telemetryCharacteristic->addDescriptor(new BLE2902());
  g_healthCharacteristic->addDescriptor(new BLE2902());
  g_debugJsonCharacteristic->addDescriptor(new BLE2902());

  service->start();

  BLEAdvertising *advertising = BLEDevice::getAdvertising();
  advertising->addServiceUUID(kBleServiceUuid);
  advertising->start();
}

void setupImpl() {
  Serial.begin(115200);
  delay(200);

  pinMode(g_nextButton.pin, INPUT_PULLUP);
  pinMode(g_prevButton.pin, INPUT_PULLUP);
  pinMode(kTempAdcPin, INPUT);

#if defined(ARDUINO_ARCH_ESP32)
  analogReadResolution(12);
  analogSetPinAttenuation(kTempAdcPin, ADC_11db);
#endif

  Wire.begin(kI2cSdaPin, kI2cSclPin);

  if (g_display.begin(SSD1306_SWITCHCAPVCC, kOledI2cAddress)) {
    g_display.clearDisplay();
    g_display.setTextColor(SSD1306_WHITE);
    g_display.setTextSize(1);
    g_display.setCursor(0, 0);
    g_display.println("HS ESP32-C3 INIT...");
    g_display.display();
  }

  g_inaReady = g_ina219.begin(&Wire);
  g_rtcReady = g_rtc.begin(&Wire);

  if (g_rtcReady) {
    Serial.println("Syncing RTC with PC build time...");
    g_rtc.adjust(DateTime(F(__DATE__), F(__TIME__)));
  }

  g_pulseReady = g_pulseSensor.begin(Wire, I2C_SPEED_FAST, kMax3010xAddress);
  if (g_pulseReady) {
    const byte ledBrightness = 0x24; // ~7.6mA
    const byte sampleAverage = 1;    // No hardware averaging for sharpest peaks
    const byte ledMode = 2;          // Red + IR
    const int sampleRate = kPulseSampleRateHz;
    const int pulseWidth = 411;
    const int adcRange = 4096;
    g_pulseSensor.setup(ledBrightness, sampleAverage, ledMode, sampleRate,
                        pulseWidth, adcRange);
    g_pulseSensor.setPulseAmplitudeGreen(0);
    g_pulseSensor.setPulseAmplitudeRed(0x24);
    g_pulseSensor.setPulseAmplitudeIR(0x24);
  }

  g_bmi160 = new DFRobot_BMI160();
  g_imuReady = (g_bmi160->I2cInit(kBmi160Address) == 0);
  if (!g_imuReady) {
    delete g_bmi160;
    g_bmi160 = nullptr;
  }

  calibrateSensors();
  setupBle();

  g_lastSensorMs = millis();
  g_lastUiMs = g_lastSensorMs;
  g_lastPacketMs = g_lastSensorMs;
  g_lastPhysioMs = g_lastSensorMs - kPhysioPeriodMs;

  Serial.println("System initialized.");
  Serial.printf("I2C SDA=%u SCL=%u\n", kI2cSdaPin, kI2cSclPin);
  Serial.printf("ADDR INA=0x%02X RTC=0x%02X BMI=0x%02X MAX=0x%02X\n",
                kIna219Address, kRtcI2cAddress, kBmi160Address,
                kMax3010xAddress);
  Serial.printf("INA219: %s | RTC: %s | BMI160: %s | MAX3010x: %s\n",
                g_inaReady ? "OK" : "MISSING", g_rtcReady ? "OK" : "MISSING",
                g_imuReady ? "OK" : "MISSING", g_pulseReady ? "OK" : "MISSING");
}

void loopImpl() {
  const uint32_t nowMs = millis();

  if (updateButton(g_nextButton, nowMs)) {
    g_activeTab = (g_activeTab + 1) % 4;
  }

  if (updateButton(g_prevButton, nowMs)) {
    g_activeTab = (g_activeTab + 3) % 4;
  }

  if ((nowMs - g_lastSensorMs) >= kSensorPeriodMs) {
    const float dtSeconds =
        static_cast<float>(nowMs - g_lastSensorMs) / 1000.0f;
    g_lastSensorMs = nowMs;

    updateImuReadings();
    updatePowerReadings();
    updatePulseReadings(nowMs);
    updatePhysioReadings(nowMs);

    updateOrientation(dtSeconds);
    updateDerivedMetrics(dtSeconds);
    updateFlightAndRisk(nowMs);
    updateCpuLoad(nowMs);
  }

  if ((nowMs - g_lastUiMs) >= kUiPeriodMs) {
    g_lastUiMs = nowMs;
    updateDisplay();
  }

  if ((nowMs - g_lastPacketMs) >= kPacketPeriodMs) {
    g_lastPacketMs = nowMs;
    sendPackets(nowMs);
  }

  delay(1);
}

} // namespace

void setup() { setupImpl(); }

void loop() { loopImpl(); }
