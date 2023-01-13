package com.google.cloud.spanner.myadapter.translator;

public abstract class TranslationOverride {

  private OverrideType overrideType;

  public TranslationOverride(OverrideType overrideType) {
    this.overrideType = overrideType;
  }

  public OverrideType getOverrideType() {
    return overrideType;
  }
}
