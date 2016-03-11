package com.imaginea

import twitter4j.conf.{Configuration, ConfigurationBuilder}

object ConfigurationBuilderUtil {
  /** consumer credentials */
  val consumerKey = "X3LoqzZzGOxrdWHWzwIDTRCtw"
  val consumerSecret = "Knm8W31dOSsxDwuMRxdoWvOrQHyTcMYvn0ASS5AEagv1ObMTDO"
  val accessToken = "574247165-7fWd1iyEfYS3m7XBz9gZ62Tyogydofaf5QWDOpdl"
  val accessSecret = "AC8SAU8ttkRREWp04rtbR9IIgOK7SysADLq3GJRVywiOI"

  /**
   * configuration builder to hold credentials
   * @return Configuration
   */
  def buildConfiguration: Configuration =
    buildConfiguration(consumerKey, consumerSecret, accessToken, accessSecret)

  /**
   *
   * @param consumerKey
   * @param consumerSecret
   * @param accessToken
   * @param accessSecret
   * @return Configuration
   */
  def buildConfiguration(consumerKey: String, consumerSecret: String,
    accessToken: String, accessSecret: String): Configuration = {
    val configurationBuilder = new ConfigurationBuilder()
    configurationBuilder.setOAuthConsumerKey(consumerKey)
    configurationBuilder.setOAuthConsumerSecret(consumerSecret)
    configurationBuilder.setOAuthAccessToken(accessToken)
    configurationBuilder.setOAuthAccessTokenSecret(accessSecret)
    configurationBuilder.setUserStreamRepliesAllEnabled(true)
    configurationBuilder.setJSONStoreEnabled(true)
    configurationBuilder.build
  }

}
