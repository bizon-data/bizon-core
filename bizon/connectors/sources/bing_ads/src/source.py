"""Microsoft Advertising (Bing Ads) accounts, Bulk campaign entities and daily reports, from the v13 REST API.

Auth defaults to a Google OAuth refresh token (`IdentityProvider: Google`, no Entra app needed); set
identity_provider=Microsoft for an Entra app and a Microsoft refresh token (scope msads.manage).

Microsoft refuses some column combinations without naming the culprit, so the column sets below were probed.
Impression-share columns cannot share a request with network, match-type and device-OS segments, hence the
`*_impression_share` streams. A column the API later rejects is dropped at runtime.
"""

import csv
import hashlib
import io
import json
import re
import time
import zipfile
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple

import requests
from loguru import logger
from pydantic import Field, model_validator
from requests.auth import AuthBase

from bizon.source.config import HttpConfig, SourceConfig
from bizon.source.models import SourceIncrementalState, SourceIteration, SourceRecord
from bizon.source.source import AbstractSource

REPORTING_URL = "https://reporting.api.bingads.microsoft.com/Reporting/v13"
BULK_URL = "https://bulk.api.bingads.microsoft.com/Bulk/v13"
CUSTOMER_URL = "https://clientcenter.api.bingads.microsoft.com/CustomerManagement/v13"
GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
MICROSOFT_TOKEN_URL = "https://login.microsoftonline.com/common/oauth2/v2.0/token"
TOKEN_URLS = (GOOGLE_TOKEN_URL, MICROSOFT_TOKEN_URL)

POLL_INTERVAL_SECONDS = 5
POLL_TIMEOUT_SECONDS = 1800

# Requesting every DownloadEntity returns InternalError; this is a verified working subset.
# fmt: off
BULK_ENTITIES = [
    "Campaigns", "AdGroups", "Ads", "Keywords", "CampaignNegativeKeywords", "AdGroupNegativeKeywords",
    "CampaignTargetCriterions", "AdGroupTargetCriterions", "CampaignNegativeSites", "AdGroupNegativeSites",
    "NegativeKeywordLists", "SharedNegativeKeywords", "CampaignNegativeKeywordListAssociations", "Budgets",
    "BidStrategies", "Labels", "CampaignLabels", "AdGroupLabels", "KeywordLabels", "SitelinkAdExtensions",
    "CampaignSitelinkAdExtensions", "CalloutAdExtensions", "CampaignCalloutAdExtensions",
    "StructuredSnippetAdExtensions", "ImageAdExtensions", "Audiences", "AdGroupAudienceAssociations",
    "CampaignAudienceAssociations", "RemarketingLists", "CustomerLists", "CombinedLists", "CustomAudiences",
    "InMarketAudiences", "AssetGroups", "AudienceGroups", "ResponsiveAds", "ResponsiveSearchAds",
    "DynamicSearchAds", "AdGroupDynamicSearchAdTargets", "Images", "Experiments", "ConversionGoal", "EventGoal",
    "UrlGoal", "OfflineConversionGoal", "DurationGoal", "PagesViewedPerVisitGoal", "CampaignConversionGoal",
    "CompanyList", "CompanyItem", "AccountNegativeKeywordList", "AccountNegativeKeyword",
    "SeasonalityAdjustments", "DataExclusions",
]

REPORT_SPECS: Dict[str, Tuple[str, List[str]]] = {
    "account_performance": (
        "AccountPerformance",
        [
            "TimePeriod", "AccountId", "Impressions", "AccountName", "AccountNumber", "CurrencyCode",
            "AdDistribution", "Clicks", "Ctr", "AverageCpc", "Spend", "AveragePosition", "Conversions",
            "ConversionRate", "CostPerConversion", "LowQualityClicks", "LowQualityClicksPercent",
            "LowQualityImpressions", "LowQualityImpressionsPercent", "LowQualityConversions",
            "LowQualityConversionRate", "DeviceType", "DeviceOS", "PhoneImpressions", "PhoneCalls",
            "Ptr", "Network", "TopVsOther", "BidMatchType", "DeliveredMatchType", "Assists", "Revenue",
            "ReturnOnAdSpend", "CostPerAssist", "RevenuePerConversion", "RevenuePerAssist",
            "AccountStatus", "LowQualityGeneralClicks", "LowQualitySophisticatedClicks", "CustomerId",
            "CustomerName", "AllConversions", "AllRevenue", "AllConversionRate", "AllCostPerConversion",
            "AllReturnOnAdSpend", "AllRevenuePerConversion", "ViewThroughConversions", "Goal",
            "GoalType", "AverageCpm", "ConversionsQualified", "LowQualityConversionsQualified",
            "AllConversionsQualified", "ViewThroughConversionsQualified", "ViewThroughRevenue",
            "VideoViews", "ViewThroughRate", "AverageCPV", "VideoViewsAt25Percent",
            "VideoViewsAt50Percent", "VideoViewsAt75Percent", "CompletedVideoViews",
            "VideoCompletionRate", "TotalWatchTimeInMS", "AverageWatchTimePerVideoView",
            "AverageWatchTimePerImpression", "Sales", "CostPerSale", "RevenuePerSale", "Installs",
            "CostPerInstall", "RevenuePerInstall", "Downloads", "PostClickDownloadRate",
            "CostPerDownload", "AppInstalls", "PostClickInstallRate", "CPI", "Purchases",
            "PostInstallPurchaseRate", "CPP", "Subscriptions", "PostInstallSubscriptionRate", "CPS",
            "NewCustomerConversions", "NewCustomerRevenue", "NewCustomerConversionRate",
            "NewCustomerCPA", "NewCustomerReturnOnAdSpend", "ConversionDelay",
            "UnknownCustomerConversions", "UnknownCustomerRevenue", "NewCustomerCount",
            "NewCustomerSpend"
        ],
    ),
    "account_impression_share": (
        "AccountPerformance",
        [
            "TimePeriod", "AccountId", "Impressions", "ImpressionSharePercent",
            "ImpressionLostToBudgetPercent", "ImpressionLostToRankAggPercent",
            "ExactMatchImpressionSharePercent", "ClickSharePercent",
            "AbsoluteTopImpressionSharePercent", "TopImpressionShareLostToRankPercent",
            "TopImpressionShareLostToBudgetPercent", "AbsoluteTopImpressionShareLostToRankPercent",
            "AbsoluteTopImpressionShareLostToBudgetPercent", "TopImpressionSharePercent",
            "AbsoluteTopImpressionRatePercent", "TopImpressionRatePercent",
            "AudienceImpressionSharePercent", "AudienceImpressionLostToRankPercent",
            "AudienceImpressionLostToBudgetPercent", "AccountName", "AccountNumber", "CurrencyCode",
            "AdDistribution", "Clicks", "Ctr", "AverageCpc", "Spend", "AveragePosition", "Conversions",
            "ConversionRate", "CostPerConversion", "LowQualityClicks", "LowQualityClicksPercent",
            "LowQualityImpressions", "LowQualityImpressionsPercent", "LowQualityConversions",
            "LowQualityConversionRate", "DeviceType", "PhoneImpressions", "PhoneCalls", "Ptr",
            "Network", "Assists", "Revenue", "ReturnOnAdSpend", "CostPerAssist", "RevenuePerConversion",
            "RevenuePerAssist", "AccountStatus", "LowQualityGeneralClicks",
            "LowQualitySophisticatedClicks", "CustomerId", "CustomerName", "AllConversions",
            "AllRevenue", "AllConversionRate", "AllCostPerConversion", "AllReturnOnAdSpend",
            "AllRevenuePerConversion", "ViewThroughConversions", "AverageCpm", "ConversionsQualified",
            "LowQualityConversionsQualified", "AllConversionsQualified",
            "ViewThroughConversionsQualified", "ViewThroughRevenue", "VideoViews", "ViewThroughRate",
            "AverageCPV", "VideoViewsAt25Percent", "VideoViewsAt50Percent", "VideoViewsAt75Percent",
            "CompletedVideoViews", "VideoCompletionRate", "TotalWatchTimeInMS",
            "AverageWatchTimePerVideoView", "AverageWatchTimePerImpression", "Sales", "CostPerSale",
            "RevenuePerSale", "Installs", "CostPerInstall", "RevenuePerInstall", "Downloads",
            "PostClickDownloadRate", "CostPerDownload", "AppInstalls", "PostClickInstallRate", "CPI",
            "Purchases", "PostInstallPurchaseRate", "CPP", "Subscriptions",
            "PostInstallSubscriptionRate", "CPS", "NewCustomerConversions", "NewCustomerRevenue",
            "NewCustomerConversionRate", "NewCustomerCPA", "NewCustomerReturnOnAdSpend",
            "ConversionDelay", "UnknownCustomerConversions", "UnknownCustomerRevenue",
            "NewCustomerCount", "NewCustomerSpend"
        ],
    ),
    "campaign_performance": (
        "CampaignPerformance",
        [
            "TimePeriod", "AccountId", "Impressions", "AccountName", "AccountNumber", "CampaignStatus",
            "CampaignName", "CampaignId", "CurrencyCode", "AdDistribution", "Clicks", "Ctr",
            "AverageCpc", "Spend", "AveragePosition", "Conversions", "ConversionRate",
            "CostPerConversion", "LowQualityClicks", "LowQualityClicksPercent", "LowQualityImpressions",
            "LowQualityImpressionsPercent", "LowQualityConversions", "LowQualityConversionRate",
            "DeviceType", "DeviceOS", "QualityScore", "ExpectedCtr", "AdRelevance",
            "LandingPageExperience", "HistoricalQualityScore", "HistoricalExpectedCtr",
            "HistoricalAdRelevance", "HistoricalLandingPageExperience", "PhoneImpressions",
            "PhoneCalls", "Ptr", "Network", "TopVsOther", "BidMatchType", "DeliveredMatchType",
            "Assists", "Revenue", "ReturnOnAdSpend", "CostPerAssist", "RevenuePerConversion",
            "RevenuePerAssist", "TrackingTemplate", "CustomParameters", "AccountStatus", "BudgetName",
            "BudgetStatus", "BudgetAssociationStatus", "LowQualityGeneralClicks",
            "LowQualitySophisticatedClicks", "CampaignLabels", "CustomerId", "CustomerName",
            "FinalUrlSuffix", "CampaignType", "BaseCampaignId", "AllConversions", "AllRevenue",
            "AllConversionRate", "AllCostPerConversion", "AllReturnOnAdSpend",
            "AllRevenuePerConversion", "ViewThroughConversions", "Goal", "GoalType", "AverageCpm",
            "ConversionsQualified", "LowQualityConversionsQualified", "AllConversionsQualified",
            "ViewThroughConversionsQualified", "ViewThroughRevenue", "VideoViews", "ViewThroughRate",
            "AverageCPV", "VideoViewsAt25Percent", "VideoViewsAt50Percent", "VideoViewsAt75Percent",
            "CompletedVideoViews", "VideoCompletionRate", "TotalWatchTimeInMS",
            "AverageWatchTimePerVideoView", "AverageWatchTimePerImpression", "Sales", "CostPerSale",
            "RevenuePerSale", "Installs", "CostPerInstall", "RevenuePerInstall", "Downloads",
            "PostClickDownloadRate", "CostPerDownload", "AppInstalls", "PostClickInstallRate", "CPI",
            "Purchases", "PostInstallPurchaseRate", "CPP", "Subscriptions",
            "PostInstallSubscriptionRate", "CPS", "NewCustomerConversions", "NewCustomerRevenue",
            "NewCustomerConversionRate", "NewCustomerCPA", "NewCustomerReturnOnAdSpend",
            "ConversionDelay", "UnknownCustomerConversions", "UnknownCustomerRevenue",
            "NewCustomerCount", "NewCustomerSpend", "MarketingObjective", "Channel", "SubChannel"
        ],
    ),
    "campaign_impression_share": (
        "CampaignPerformance",
        [
            "TimePeriod", "AccountId", "Impressions", "ImpressionSharePercent",
            "ImpressionLostToBudgetPercent", "ImpressionLostToRankAggPercent",
            "ExactMatchImpressionSharePercent", "ClickSharePercent",
            "AbsoluteTopImpressionSharePercent", "TopImpressionShareLostToRankPercent",
            "TopImpressionShareLostToBudgetPercent", "AbsoluteTopImpressionShareLostToRankPercent",
            "AbsoluteTopImpressionShareLostToBudgetPercent", "TopImpressionSharePercent",
            "AbsoluteTopImpressionRatePercent", "TopImpressionRatePercent",
            "AudienceImpressionSharePercent", "AudienceImpressionLostToRankPercent",
            "AudienceImpressionLostToBudgetPercent", "RelativeCtr", "AccountName", "AccountNumber",
            "CampaignStatus", "CampaignName", "CampaignId", "CurrencyCode", "AdDistribution", "Clicks",
            "Ctr", "AverageCpc", "Spend", "AveragePosition", "Conversions", "ConversionRate",
            "CostPerConversion", "LowQualityClicks", "LowQualityClicksPercent", "LowQualityImpressions",
            "LowQualityImpressionsPercent", "LowQualityConversions", "LowQualityConversionRate",
            "DeviceType", "QualityScore", "ExpectedCtr", "AdRelevance", "LandingPageExperience",
            "HistoricalQualityScore", "HistoricalExpectedCtr", "HistoricalAdRelevance",
            "HistoricalLandingPageExperience", "PhoneImpressions", "PhoneCalls", "Ptr", "Network",
            "Assists", "Revenue", "ReturnOnAdSpend", "CostPerAssist", "RevenuePerConversion",
            "RevenuePerAssist", "TrackingTemplate", "CustomParameters", "AccountStatus",
            "LowQualityGeneralClicks", "LowQualitySophisticatedClicks", "CampaignLabels", "CustomerId",
            "CustomerName", "FinalUrlSuffix", "CampaignType", "BaseCampaignId", "AllConversions",
            "AllRevenue", "AllConversionRate", "AllCostPerConversion", "AllReturnOnAdSpend",
            "AllRevenuePerConversion", "ViewThroughConversions", "AverageCpm", "ConversionsQualified",
            "LowQualityConversionsQualified", "AllConversionsQualified",
            "ViewThroughConversionsQualified", "ViewThroughRevenue", "VideoViews", "ViewThroughRate",
            "AverageCPV", "VideoViewsAt25Percent", "VideoViewsAt50Percent", "VideoViewsAt75Percent",
            "CompletedVideoViews", "VideoCompletionRate", "TotalWatchTimeInMS",
            "AverageWatchTimePerVideoView", "AverageWatchTimePerImpression", "Sales", "CostPerSale",
            "RevenuePerSale", "Installs", "CostPerInstall", "RevenuePerInstall", "Downloads",
            "PostClickDownloadRate", "CostPerDownload", "AppInstalls", "PostClickInstallRate", "CPI",
            "Purchases", "PostInstallPurchaseRate", "CPP", "Subscriptions",
            "PostInstallSubscriptionRate", "CPS", "NewCustomerConversions", "NewCustomerRevenue",
            "NewCustomerConversionRate", "NewCustomerCPA", "NewCustomerReturnOnAdSpend",
            "ConversionDelay", "UnknownCustomerConversions", "UnknownCustomerRevenue",
            "NewCustomerCount", "NewCustomerSpend", "MarketingObjective", "Channel", "SubChannel"
        ],
    ),
    "ad_group_performance": (
        "AdGroupPerformance",
        [
            "TimePeriod", "AccountId", "Impressions", "AccountName", "AccountNumber", "Status",
            "CampaignName", "CampaignId", "AdGroupName", "AdGroupId", "CurrencyCode", "AdDistribution",
            "Clicks", "Ctr", "AverageCpc", "Spend", "AveragePosition", "Conversions", "ConversionRate",
            "CostPerConversion", "DeviceType", "Language", "DeviceOS", "QualityScore", "ExpectedCtr",
            "AdRelevance", "LandingPageExperience", "HistoricalQualityScore", "HistoricalExpectedCtr",
            "HistoricalAdRelevance", "HistoricalLandingPageExperience", "PhoneImpressions",
            "PhoneCalls", "Ptr", "Network", "TopVsOther", "BidMatchType", "DeliveredMatchType",
            "Assists", "Revenue", "ReturnOnAdSpend", "CostPerAssist", "RevenuePerConversion",
            "RevenuePerAssist", "TrackingTemplate", "CustomParameters", "AccountStatus",
            "CampaignStatus", "AdGroupLabels", "CustomerId", "CustomerName", "FinalUrlSuffix",
            "CampaignType", "BaseCampaignId", "AllConversions", "AllRevenue", "AllConversionRate",
            "AllCostPerConversion", "AllReturnOnAdSpend", "AllRevenuePerConversion",
            "ViewThroughConversions", "Goal", "GoalType", "AdGroupType", "AverageCpm",
            "ConversionsQualified", "AllConversionsQualified", "ViewThroughConversionsQualified",
            "ViewThroughRevenue", "VideoViews", "ViewThroughRate", "AverageCPV",
            "VideoViewsAt25Percent", "VideoViewsAt50Percent", "VideoViewsAt75Percent",
            "CompletedVideoViews", "VideoCompletionRate", "TotalWatchTimeInMS",
            "AverageWatchTimePerVideoView", "AverageWatchTimePerImpression", "Sales", "CostPerSale",
            "RevenuePerSale", "Installs", "CostPerInstall", "RevenuePerInstall", "GoalId",
            "MarketingObjective", "Channel", "SubChannel"
        ],
    ),
    "ad_group_impression_share": (
        "AdGroupPerformance",
        [
            "TimePeriod", "AccountId", "Impressions", "ImpressionSharePercent",
            "ImpressionLostToBudgetPercent", "ImpressionLostToRankAggPercent",
            "ExactMatchImpressionSharePercent", "ClickSharePercent",
            "AbsoluteTopImpressionSharePercent", "TopImpressionShareLostToRankPercent",
            "TopImpressionShareLostToBudgetPercent", "AbsoluteTopImpressionShareLostToRankPercent",
            "AbsoluteTopImpressionShareLostToBudgetPercent", "TopImpressionSharePercent",
            "AbsoluteTopImpressionRatePercent", "TopImpressionRatePercent",
            "AudienceImpressionSharePercent", "AudienceImpressionLostToRankPercent",
            "AudienceImpressionLostToBudgetPercent", "RelativeCtr", "AccountName", "AccountNumber",
            "Status", "CampaignName", "CampaignId", "AdGroupName", "AdGroupId", "CurrencyCode",
            "AdDistribution", "Clicks", "Ctr", "AverageCpc", "Spend", "AveragePosition", "Conversions",
            "ConversionRate", "CostPerConversion", "DeviceType", "Language", "QualityScore",
            "ExpectedCtr", "AdRelevance", "LandingPageExperience", "HistoricalQualityScore",
            "HistoricalExpectedCtr", "HistoricalAdRelevance", "HistoricalLandingPageExperience",
            "PhoneImpressions", "PhoneCalls", "Ptr", "Network", "Assists", "Revenue", "ReturnOnAdSpend",
            "CostPerAssist", "RevenuePerConversion", "RevenuePerAssist", "TrackingTemplate",
            "CustomParameters", "AccountStatus", "CampaignStatus", "AdGroupLabels", "CustomerId",
            "CustomerName", "FinalUrlSuffix", "CampaignType", "BaseCampaignId", "AllConversions",
            "AllRevenue", "AllConversionRate", "AllCostPerConversion", "AllReturnOnAdSpend",
            "AllRevenuePerConversion", "ViewThroughConversions", "AdGroupType", "AverageCpm",
            "ConversionsQualified", "AllConversionsQualified", "ViewThroughConversionsQualified",
            "ViewThroughRevenue", "VideoViews", "ViewThroughRate", "AverageCPV",
            "VideoViewsAt25Percent", "VideoViewsAt50Percent", "VideoViewsAt75Percent",
            "CompletedVideoViews", "VideoCompletionRate", "TotalWatchTimeInMS",
            "AverageWatchTimePerVideoView", "AverageWatchTimePerImpression", "Sales", "CostPerSale",
            "RevenuePerSale", "Installs", "CostPerInstall", "RevenuePerInstall", "MarketingObjective",
            "Channel", "SubChannel"
        ],
    ),
    "ad_performance": (
        "AdPerformance",
        [
            "TimePeriod", "AccountId", "Impressions", "AccountName", "AccountNumber", "CampaignName",
            "CampaignId", "AdGroupName", "AdId", "AdGroupId", "AdTitle", "AdDescription",
            "AdDescription2", "AdType", "CurrencyCode", "AdDistribution", "Clicks", "Ctr", "AverageCpc",
            "Spend", "AveragePosition", "Conversions", "ConversionRate", "CostPerConversion",
            "DestinationUrl", "DeviceType", "Language", "DisplayUrl", "AdStatus", "Network",
            "TopVsOther", "BidMatchType", "DeliveredMatchType", "DeviceOS", "Assists", "Revenue",
            "ReturnOnAdSpend", "CostPerAssist", "RevenuePerConversion", "RevenuePerAssist",
            "TrackingTemplate", "CustomParameters", "FinalUrl", "FinalMobileUrl", "FinalAppUrl",
            "AccountStatus", "CampaignStatus", "AdGroupStatus", "TitlePart1", "TitlePart2",
            "TitlePart3", "Headline", "LongHeadline", "BusinessName", "Path1", "Path2", "AdLabels",
            "CustomerId", "CustomerName", "CampaignType", "BaseCampaignId", "AllConversions",
            "AllRevenue", "AllConversionRate", "AllCostPerConversion", "AllReturnOnAdSpend",
            "AllRevenuePerConversion", "FinalUrlSuffix", "ViewThroughConversions", "Goal", "GoalType",
            "AbsoluteTopImpressionRatePercent", "TopImpressionRatePercent", "AverageCpm",
            "ConversionsQualified", "AllConversionsQualified", "ViewThroughConversionsQualified",
            "ViewThroughRevenue", "VideoViews", "ViewThroughRate", "AverageCPV",
            "VideoViewsAt25Percent", "VideoViewsAt50Percent", "VideoViewsAt75Percent",
            "CompletedVideoViews", "VideoCompletionRate", "TotalWatchTimeInMS",
            "AverageWatchTimePerVideoView", "AverageWatchTimePerImpression", "AdStrength",
            "AdStrengthActionItems", "GoalId", "MarketingObjective", "Channel", "SubChannel"
        ],
    ),
    "keyword_performance": (
        "KeywordPerformance",
        [
            "TimePeriod", "AccountId", "Impressions", "AccountName", "AccountNumber", "CampaignName",
            "CampaignId", "AdGroupName", "AdGroupId", "Keyword", "KeywordId", "AdId", "AdType",
            "DestinationUrl", "CurrentMaxCpc", "CurrencyCode", "DeliveredMatchType", "AdDistribution",
            "Clicks", "Ctr", "AverageCpc", "Spend", "AveragePosition", "Conversions", "ConversionRate",
            "CostPerConversion", "BidMatchType", "DeviceType", "QualityScore", "ExpectedCtr",
            "AdRelevance", "LandingPageExperience", "Language", "HistoricalQualityScore",
            "HistoricalExpectedCtr", "HistoricalAdRelevance", "HistoricalLandingPageExperience",
            "QualityImpact", "CampaignStatus", "AccountStatus", "AdGroupStatus", "KeywordStatus",
            "Network", "TopVsOther", "DeviceOS", "Assists", "Revenue", "ReturnOnAdSpend",
            "CostPerAssist", "RevenuePerConversion", "RevenuePerAssist", "TrackingTemplate",
            "CustomParameters", "FinalUrl", "FinalMobileUrl", "FinalAppUrl", "BidStrategyType",
            "KeywordLabels", "Mainline1Bid", "MainlineBid", "FirstPageBid", "FinalUrlSuffix",
            "BaseCampaignId", "AllConversions", "AllRevenue", "AllConversionRate",
            "AllCostPerConversion", "AllReturnOnAdSpend", "AllRevenuePerConversion",
            "ViewThroughConversions", "Goal", "GoalType", "AbsoluteTopImpressionRatePercent",
            "TopImpressionRatePercent", "AverageCpm", "ConversionsQualified", "AllConversionsQualified",
            "ViewThroughConversionsQualified", "ViewThroughRevenue", "GoalId", "MarketingObjective",
            "Channel", "SubChannel"
        ],
    ),
    "search_query_performance": (
        "SearchQueryPerformance",
        [
            "TimePeriod", "AccountId", "SearchQuery", "Impressions", "AccountName", "AccountNumber",
            "CampaignName", "CampaignId", "AdGroupName", "AdGroupId", "AdId", "AdType",
            "DestinationUrl", "BidMatchType", "DeliveredMatchType", "CampaignStatus", "AdStatus",
            "Clicks", "Ctr", "AverageCpc", "Spend", "AveragePosition", "Keyword", "AdGroupCriterionId",
            "Conversions", "ConversionRate", "CostPerConversion", "Language", "KeywordId", "Network",
            "TopVsOther", "DeviceType", "DeviceOS", "Assists", "Revenue", "ReturnOnAdSpend",
            "CostPerAssist", "RevenuePerConversion", "RevenuePerAssist", "AccountStatus",
            "AdGroupStatus", "KeywordStatus", "CampaignType", "CustomerId", "CustomerName",
            "AllConversions", "AllRevenue", "AllConversionRate", "AllCostPerConversion",
            "AllReturnOnAdSpend", "AllRevenuePerConversion", "Goal", "GoalType",
            "AbsoluteTopImpressionRatePercent", "TopImpressionRatePercent", "AverageCpm",
            "ConversionsQualified", "AllConversionsQualified", "AssetGroupName", "AssetGroupId",
            "AssetGroupStatus", "MarketingObjective", "Channel", "SubChannel"
        ],
    ),
    "ms_click_id_performance": (
        "MSClickIdPerformance",
        [
            "TimePeriod", "AccountId", "MSClickId", "Clicks", "CampaignId", "AdGroupId", "AssetGroupId",
            "AdId", "SearchQuery", "Keyword", "PageNumber", "TopVsOther", "BidMatchType", "Network",
            "DeviceType", "Country", "State", "MetroArea", "City", "Cpc", "ConversionsQualified",
            "Revenue"
        ],
    ),
    "conversion_performance": (
        "ConversionPerformance",
        [
            "TimePeriod", "AccountId", "Impressions", "AccountName", "AccountNumber", "CampaignName",
            "CampaignId", "AdGroupName", "AdGroupId", "Keyword", "KeywordId", "Clicks", "Ctr",
            "Assists", "Conversions", "ConversionRate", "Spend", "Revenue", "ReturnOnAdSpend",
            "CostPerConversion", "CostPerAssist", "RevenuePerConversion", "RevenuePerAssist",
            "DeviceType", "AccountStatus", "CampaignStatus", "AdGroupStatus", "KeywordStatus",
            "AllConversions", "AllRevenue", "AllConversionRate", "AllCostPerConversion",
            "AllReturnOnAdSpend", "AllRevenuePerConversion", "ViewThroughConversions", "Goal",
            "GoalType", "ConversionsQualified", "AllConversionsQualified",
            "ViewThroughConversionsQualified", "ViewThroughRevenue", "GoalId", "MarketingObjective",
            "Channel", "SubChannel"
        ],
    ),
    "goals_and_funnels": (
        "GoalsAndFunnels",
        [
            "AccountName", "AccountNumber", "AccountId", "TimePeriod", "CampaignName", "CampaignId",
            "AdGroupName", "AdGroupId", "Keyword", "KeywordId", "Goal", "AllConversions", "Assists",
            "AllRevenue", "GoalId", "DeviceType", "DeviceOS", "AccountStatus", "CampaignStatus",
            "AdGroupStatus", "KeywordStatus", "GoalType", "ViewThroughConversions",
            "AllConversionsQualified", "ViewThroughConversionsQualified", "ViewThroughRevenue",
            "CampaignType", "AssetGroupId", "AssetGroupName", "AssetGroupStatus", "ConversionDelay",
            "MarketingObjective", "Channel", "SubChannel"
        ],
    ),
    "geographic_performance": (
        "GeographicPerformance",
        [
            "TimePeriod", "AccountId", "Country", "Impressions", "AccountName", "AccountNumber",
            "CampaignName", "CampaignId", "AdGroupName", "AdGroupId", "State", "MetroArea", "City",
            "CurrencyCode", "AdDistribution", "Clicks", "Ctr", "AverageCpc", "Spend", "AveragePosition",
            "ProximityTargetLocation", "Radius", "Language", "BidMatchType", "DeliveredMatchType",
            "Network", "TopVsOther", "DeviceType", "DeviceOS", "Assists", "Conversions",
            "ConversionRate", "Revenue", "ReturnOnAdSpend", "CostPerConversion", "CostPerAssist",
            "RevenuePerConversion", "RevenuePerAssist", "LocationType", "MostSpecificLocation",
            "AccountStatus", "CampaignStatus", "AdGroupStatus", "County", "PostalCode", "LocationId",
            "BaseCampaignId", "AllConversions", "AllRevenue", "AllConversionRate",
            "AllCostPerConversion", "AllReturnOnAdSpend", "AllRevenuePerConversion",
            "ViewThroughConversions", "Goal", "GoalType", "AbsoluteTopImpressionRatePercent",
            "TopImpressionRatePercent", "AverageCpm", "ConversionsQualified", "AllConversionsQualified",
            "ViewThroughConversionsQualified", "Neighborhood", "ViewThroughRevenue", "CampaignType",
            "AssetGroupId", "AssetGroupName", "AssetGroupStatus", "Downloads", "PostClickDownloadRate",
            "CostPerDownload", "AppInstalls", "PostClickInstallRate", "CPI", "Purchases",
            "PostInstallPurchaseRate", "CPP", "Subscriptions", "PostInstallSubscriptionRate", "CPS",
            "MarketingObjective", "Channel", "SubChannel"
        ],
    ),
    "user_location_performance": (
        "UserLocationPerformance",
        [
            "TimePeriod", "AccountId", "Country", "Impressions", "AccountName", "AccountNumber",
            "CampaignName", "CampaignId", "AdGroupName", "AdGroupId", "State", "MetroArea",
            "CurrencyCode", "AdDistribution", "Clicks", "Ctr", "AverageCpc", "Spend", "AveragePosition",
            "ProximityTargetLocation", "Radius", "Language", "City", "QueryIntentCountry",
            "QueryIntentState", "QueryIntentCity", "QueryIntentDMA", "BidMatchType",
            "DeliveredMatchType", "Network", "TopVsOther", "DeviceType", "DeviceOS", "Assists",
            "Conversions", "ConversionRate", "Revenue", "ReturnOnAdSpend", "CostPerConversion",
            "CostPerAssist", "RevenuePerConversion", "RevenuePerAssist", "County", "PostalCode",
            "QueryIntentCounty", "QueryIntentPostalCode", "LocationId", "QueryIntentLocationId",
            "AllConversions", "AllRevenue", "AllConversionRate", "AllCostPerConversion",
            "AllReturnOnAdSpend", "AllRevenuePerConversion", "ViewThroughConversions", "Goal",
            "GoalType", "AbsoluteTopImpressionRatePercent", "TopImpressionRatePercent", "AverageCpm",
            "ConversionsQualified", "AllConversionsQualified", "ViewThroughConversionsQualified",
            "Neighborhood", "QueryIntentNeighborhood", "ViewThroughRevenue", "CampaignType",
            "AssetGroupId", "AssetGroupName", "Downloads", "PostClickDownloadRate", "CostPerDownload",
            "AppInstalls", "PostClickInstallRate", "CPI", "Purchases", "PostInstallPurchaseRate", "CPP",
            "Subscriptions", "PostInstallSubscriptionRate", "CPS", "MarketingObjective", "Channel",
            "SubChannel"
        ],
    ),
    "age_gender_audience": (
        "AgeGenderAudience",
        [
            "TimePeriod", "AccountId", "AgeGroup", "Gender", "Impressions", "AccountName",
            "AccountNumber", "CampaignName", "CampaignId", "AdGroupName", "AdGroupId", "AdDistribution",
            "Clicks", "Conversions", "Spend", "Revenue", "ExtendedCost", "Assists", "Language",
            "AccountStatus", "CampaignStatus", "AdGroupStatus", "BaseCampaignId", "AllConversions",
            "AllRevenue", "ViewThroughConversions", "Goal", "GoalType",
            "AbsoluteTopImpressionRatePercent", "TopImpressionRatePercent", "ConversionsQualified",
            "AllConversionsQualified", "ViewThroughConversionsQualified", "ViewThroughRevenue",
            "CampaignType", "AssetGroupId", "AssetGroupName", "AssetGroupStatus", "MarketingObjective",
            "Channel", "SubChannel"
        ],
    ),
    "professional_demographics_audience": (
        "ProfessionalDemographicsAudience",
        [
            "AccountName", "AccountNumber", "AccountId", "TimePeriod", "CampaignName", "CampaignId",
            "AdGroupName", "AdGroupId", "CompanyName", "IndustryName", "JobFunctionName",
            "JobSeniorityName", "CustomLinkedInName", "LinkedInSegmentName", "LinkedInSegmentId",
            "LinkedInSegmentType", "AdDistribution", "Impressions", "Clicks", "Spend", "Conversions",
            "Language", "Assists", "Revenue", "AccountStatus", "CampaignStatus", "AdGroupStatus",
            "AveragePosition", "AllConversions", "AllRevenue", "ViewThroughConversions", "Goal",
            "GoalType", "AbsoluteTopImpressionRatePercent", "TopImpressionRatePercent",
            "ConversionsQualified", "AllConversionsQualified", "ViewThroughConversionsQualified",
            "ViewThroughRevenue", "CampaignType", "AssetGroupId", "AssetGroupName", "AssetGroupStatus",
            "MarketingObjective", "Channel", "SubChannel"
        ],
    ),
    "audience_performance": (
        "AudiencePerformance",
        [
            "TimePeriod", "AccountId", "AudienceId", "Impressions", "AccountName", "AccountNumber",
            "CampaignName", "CampaignId", "AdGroupName", "AdGroupId", "AssetGroupName", "AssetGroupId",
            "AudienceName", "AssociationStatus", "BidAdjustment", "TargetingSetting", "Clicks", "Ctr",
            "AverageCpc", "Spend", "AveragePosition", "Conversions", "ConversionRate",
            "CostPerConversion", "Revenue", "ReturnOnAdSpend", "RevenuePerConversion", "AccountStatus",
            "CampaignStatus", "AdGroupStatus", "AudienceType", "BaseCampaignId", "AllConversions",
            "AllRevenue", "AllConversionRate", "AllCostPerConversion", "AllReturnOnAdSpend",
            "AllRevenuePerConversion", "AssociationId", "AssociationLevel", "ViewThroughConversions",
            "Goal", "GoalType", "AbsoluteTopImpressionRatePercent", "TopImpressionRatePercent",
            "AverageCpm", "ConversionsQualified", "AllConversionsQualified",
            "ViewThroughConversionsQualified", "ViewThroughRevenue", "VideoViews", "ViewThroughRate",
            "AverageCPV", "VideoViewsAt25Percent", "VideoViewsAt50Percent", "VideoViewsAt75Percent",
            "CompletedVideoViews", "VideoCompletionRate", "TotalWatchTimeInMS",
            "AverageWatchTimePerVideoView", "AverageWatchTimePerImpression", "MarketingObjective",
            "Channel", "SubChannel"
        ],
    ),
    "destination_url_performance": (
        "DestinationUrlPerformance",
        [
            "TimePeriod", "AccountId", "DestinationUrl", "Impressions", "AccountName", "AccountNumber",
            "CampaignName", "CampaignId", "AdGroupName", "AdGroupId", "AdId", "CurrencyCode",
            "AdDistribution", "Clicks", "Ctr", "AverageCpc", "Spend", "AveragePosition", "Conversions",
            "ConversionRate", "CostPerConversion", "DeviceType", "Language", "BidMatchType",
            "DeliveredMatchType", "Network", "TopVsOther", "DeviceOS", "Assists", "Revenue",
            "ReturnOnAdSpend", "CostPerAssist", "RevenuePerConversion", "RevenuePerAssist",
            "TrackingTemplate", "CustomParameters", "FinalUrl", "FinalMobileUrl", "FinalAppUrl",
            "AccountStatus", "CampaignStatus", "AdGroupStatus", "AdStatus", "CustomerId",
            "CustomerName", "FinalUrlSuffix", "AllConversions", "AllRevenue", "AllConversionRate",
            "AllCostPerConversion", "AllReturnOnAdSpend", "AllRevenuePerConversion",
            "ViewThroughConversions", "Goal", "GoalType", "AbsoluteTopImpressionRatePercent",
            "TopImpressionRatePercent", "AverageCpm", "ConversionsQualified", "AllConversionsQualified",
            "ViewThroughConversionsQualified", "ViewThroughRevenue", "MarketingObjective", "Channel",
            "SubChannel"
        ],
    ),
    "asset_group_performance": (
        "AssetGroupPerformance",
        [
            "TimePeriod", "AccountName", "AccountId", "CampaignName", "CampaignId", "AccountStatus",
            "CampaignStatus", "AssetGroupId", "AssetGroupName", "AssetGroupStatus", "Impressions",
            "Clicks", "Ctr", "Spend", "AverageCpc", "Conversions", "Revenue", "ReturnOnAdSpend",
            "CampaignType", "CostPerConversion", "MarketingObjective", "Channel", "SubChannel"
        ],
    ),
    "asset_performance": (
        "AssetPerformance",
        [
            "TimePeriod", "AccountId", "AccountName", "CampaignId", "CampaignName", "AdGroupId",
            "AdGroupName", "AssetId", "AssetContent", "AssetType", "AssetSource", "Impressions",
            "Clicks", "Ctr", "Spend", "VideoViews", "VideoViewsAt25Percent", "VideoViewsAt50Percent",
            "VideoViewsAt75Percent", "CompletedVideoViews", "VideoCompletionRate", "Conversions",
            "Revenue", "MarketingObjective", "Channel", "SubChannel", "AssetGroupId", "AdId"
        ],
    ),
    "budget_summary": (
        "BudgetSummary",
        [
            "AccountId", "Date", "DailySpend", "AccountName", "AccountNumber", "CampaignName",
            "CampaignId", "CurrencyCode", "MonthlyBudget", "MonthToDateSpend"
        ],
    ),
    "search_campaign_change_history": (
        "SearchCampaignChangeHistory",
        [
            "DateTime", "AccountId", "ChangedBy", "ItemChanged", "HowChanged", "AccountName",
            "AccountNumber", "CampaignName", "CampaignId", "AdGroupName", "AdGroupId", "AdTitle",
            "AdDescription", "DisplayUrl", "Keyword", "AttributeChanged", "OldValue", "NewValue",
            "EntityName", "EntityId", "Tool"
        ],
    ),
}
# fmt: on


ENTITY_STREAMS = ["accounts", "bulk_entities"]

# Reports that only serve recent days: requests for older windows succeed but come back empty.
RETENTION_DAYS = {"ms_click_id_performance": 90}

BingAdsStreams = Enum(
    "BingAdsStreams",
    {name.upper(): name for name in ENTITY_STREAMS + list(REPORT_SPECS)},
    type=str,
)


class BingAdsSourceConfig(SourceConfig):
    stream: BingAdsStreams
    client_id: str
    client_secret: str
    refresh_token: str
    developer_token: str
    customer_id: str = Field(..., description="Manager/customer id that owns the accounts")
    account_ids: List[str] = Field(default_factory=list, description="Empty = every account under customer_id")
    identity_provider: str = Field("Google", description="Google or Microsoft: who issued refresh_token")
    start_date: Optional[str] = Field(
        None, description="First day of a full (non-incremental) report backfill. Required for report streams."
    )
    end_date: Optional[str] = Field(None, description="Last day to extract; defaults to today (UTC)")
    lookback_days: int = Field(30, description="Days re-read on every incremental run (conversion restatements)")
    window_days: int = Field(14, description="Days per report request")
    # Error bodies drive column pruning (BingApiError), so the session must hand non-2xx responses back.
    http: Optional[HttpConfig] = Field(
        default_factory=lambda: HttpConfig(raise_for_status=False), description="HTTP timeout/retry policy"
    )

    @model_validator(mode="after")
    def _reports_need_a_start_date(self):
        if self.stream.value in REPORT_SPECS and not self.start_date:
            raise ValueError("start_date is required for report streams")
        return self


class BingAdsAuth(AuthBase):
    """Adds the Microsoft Advertising auth headers; never touches OAuth token requests."""

    def __init__(self, source: "BingAdsSource"):
        self.source = source

    def __call__(self, request):
        if request.url.startswith(TOKEN_URLS):
            return request
        request.headers["Authorization"] = f"Bearer {self.source.access_token()}"
        request.headers["DeveloperToken"] = self.source.config.developer_token
        request.headers["CustomerId"] = str(self.source.config.customer_id)
        if self.source.config.identity_provider.lower() == "google":
            request.headers["IdentityProvider"] = "Google"
        return request


class BingAdsSource(AbstractSource):
    def __init__(self, config: BingAdsSourceConfig):
        self._token: Optional[str] = None
        self._token_expires_at = 0.0
        super().__init__(config)
        self.config: BingAdsSourceConfig = config
        self._accounts: Optional[List[str]] = None

    @staticmethod
    def streams() -> List[str]:
        return [s.value for s in BingAdsStreams]

    @staticmethod
    def get_config_class() -> type:
        return BingAdsSourceConfig

    def get_authenticator(self) -> AuthBase:
        return BingAdsAuth(self)

    def get_total_records_count(self) -> Optional[int]:
        return None

    def access_token(self) -> str:
        if self._token and time.time() < self._token_expires_at - 120:
            return self._token
        if self.config.identity_provider.lower() == "google":
            url = GOOGLE_TOKEN_URL
            data = {"grant_type": "refresh_token"}
        else:
            url = MICROSOFT_TOKEN_URL
            data = {"grant_type": "refresh_token", "scope": "https://ads.microsoft.com/msads.manage offline_access"}
        data.update(
            client_id=self.config.client_id,
            client_secret=self.config.client_secret,
            refresh_token=self.config.refresh_token,
        )
        response = self.session.post(url, data=data, timeout=(10, 60))
        response.raise_for_status()
        payload = response.json()
        self._token = payload["access_token"]
        self._token_expires_at = time.time() + int(payload.get("expires_in", 3600))
        return self._token

    def _post(self, url: str, body: dict, account_id: Optional[str] = None) -> dict:
        headers = {"CustomerAccountId": str(account_id)} if account_id else {}
        response = self.session.post(url, json=body, headers=headers, timeout=(10, 120))
        try:
            payload = response.json()
        except ValueError:
            response.raise_for_status()
            raise
        if response.status_code >= 400 or payload.get("OperationErrors"):
            raise BingApiError(response.status_code, payload)
        return payload

    def check_connection(self) -> Tuple[bool, Optional[Any]]:
        try:
            self._post(f"{CUSTOMER_URL}/User/Query", {"UserId": None})
            return True, None
        except Exception as e:
            return False, str(e)

    def _account_ids(self) -> List[str]:
        if self._accounts is None:
            if self.config.account_ids:
                self._accounts = [str(a) for a in self.config.account_ids]
            else:
                payload = self._post(
                    f"{CUSTOMER_URL}/AccountsInfo/Query",
                    {"CustomerId": str(self.config.customer_id), "OnlyParentAccounts": False},
                )
                self._accounts = sorted(str(a["Id"]) for a in payload.get("AccountsInfo") or [])
            logger.info(f"Microsoft Advertising accounts: {self._accounts}")
        return self._accounts

    def _get_accounts(self) -> SourceIteration:
        records = []
        for account_id in self._account_ids():
            account = self._post(f"{CUSTOMER_URL}/Account/Query", {"AccountId": int(account_id)}).get("Account") or {}
            records.append(SourceRecord(id=str(account_id), data=account))
        return SourceIteration(records=records, next_pagination={})

    @staticmethod
    def _download_csv(url: str) -> List[Dict[str, str]]:
        # Not self.session: the URL is pre-signed and must not carry the Authorization header.
        response = requests.get(url, timeout=(10, 600))
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
            text = archive.read(archive.namelist()[0]).decode("utf-8-sig")
        return list(csv.DictReader(io.StringIO(text)))

    def _get_bulk_entities(self, pagination: Optional[dict]) -> SourceIteration:
        accounts = self._account_ids()
        idx = (pagination or {}).get("account_idx", 0)
        if idx >= len(accounts):
            return SourceIteration(records=[], next_pagination={})
        account_id = accounts[idx]
        body = {
            "AccountIds": [int(account_id)],
            "CompressionType": "Zip",
            "DataScope": "EntityData",
            "DownloadEntities": BULK_ENTITIES,
            "DownloadFileType": "Csv",
            "FormatVersion": "6.0",
        }
        request_id = self._post(f"{BULK_URL}/Campaigns/DownloadByAccountIds", body, account_id)["DownloadRequestId"]
        deadline = time.time() + POLL_TIMEOUT_SECONDS
        while True:
            status = self._post(f"{BULK_URL}/BulkDownloadStatus/Query", {"RequestId": request_id}, account_id)
            if status.get("RequestStatus") != "InProgress":
                break
            if time.time() > deadline:
                raise TimeoutError(f"Bulk download {request_id} still in progress after {POLL_TIMEOUT_SECONDS}s")
            time.sleep(POLL_INTERVAL_SECONDS)
        if status.get("RequestStatus") != "Completed" or not status.get("ResultFileUrl"):
            raise RuntimeError(
                f"Bulk download {request_id} ended {status.get('RequestStatus')}: {status.get('Errors')}"
            )

        records = []
        for row_number, row in enumerate(self._download_csv(status["ResultFileUrl"])):
            # The Bulk file is one 580+ column sheet shared by every entity type; keep only populated cells.
            data = {k: v for k, v in row.items() if v not in ("", None)}
            if not data.get("Type"):
                continue
            key = data.get("Id") or hashlib.sha1(json.dumps(data, sort_keys=True).encode()).hexdigest()
            data["_account_id"] = account_id
            data["_row_number"] = row_number
            records.append(SourceRecord(id=f"{account_id}|{data['Type']}|{data.get('Parent Id', '')}|{key}", data=data))
        logger.info(f"bulk_entities: {len(records)} rows for account {account_id}")
        next_pagination = {"account_idx": idx + 1} if idx + 1 < len(accounts) else {}
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _end_date(self) -> date:
        if self.config.end_date:
            return date.fromisoformat(self.config.end_date)
        return datetime.now(timezone.utc).date()

    @staticmethod
    def _bing_date(d: date) -> dict:
        return {"Day": d.day, "Month": d.month, "Year": d.year}

    def _submit_report(self, report_type: str, columns: List[str], account_id: str, start: date, end: date) -> str:
        columns = list(columns)
        for _ in range(len(columns)):
            body = {
                "ReportRequest": {
                    "Type": f"{report_type}ReportRequest",
                    "Format": "Csv",
                    "FormatVersion": "2.0",
                    "ExcludeReportHeader": True,
                    "ExcludeReportFooter": True,
                    "ExcludeColumnHeaders": False,
                    "ReturnOnlyCompleteData": False,
                    "Aggregation": "Daily",
                    "Columns": columns,
                    "Scope": {"AccountIds": [int(account_id)]},
                    "Time": {
                        "CustomDateRangeStart": self._bing_date(start),
                        "CustomDateRangeEnd": self._bing_date(end),
                    },
                }
            }
            try:
                return self._post(f"{REPORTING_URL}/GenerateReport/Submit", body, account_id)["ReportRequestId"]
            except BingApiError as e:
                bad = e.invalid_column_index()
                if bad is None or bad >= len(columns):
                    raise
                logger.warning(f"{report_type}: API rejected column {columns[bad]}; dropping it")
                columns.pop(bad)
        raise RuntimeError(f"{report_type}: no accepted column set")

    def _poll_report(self, request_id: str, account_id: str) -> Optional[str]:
        deadline = time.time() + POLL_TIMEOUT_SECONDS
        while True:
            status = self._post(f"{REPORTING_URL}/GenerateReport/Poll", {"ReportRequestId": request_id}, account_id)
            report = status.get("ReportRequestStatus") or {}
            if report.get("Status") == "Success":
                # Success with no URL means the report is empty.
                return report.get("ReportDownloadUrl")
            if report.get("Status") == "Error":
                raise RuntimeError(f"Report {request_id} failed: {json.dumps(status)[:1000]}")
            if time.time() > deadline:
                raise TimeoutError(f"Report {request_id} still pending after {POLL_TIMEOUT_SECONDS}s")
            time.sleep(POLL_INTERVAL_SECONDS)

    def _get_report(self, start: date, pagination: Optional[dict]) -> SourceIteration:
        stream = self.config.stream.value
        report_type, columns = REPORT_SPECS[stream]
        end = self._end_date()
        if stream in RETENTION_DAYS:
            start = max(start, end - timedelta(days=RETENTION_DAYS[stream] - 1))
        accounts = self._account_ids()
        pagination = pagination or {}
        account_idx = pagination.get("account_idx", 0)
        # A resumed run recomputes `end` from today, so the cursor stores a date, not an index.
        window_start = date.fromisoformat(pagination["window_start"]) if pagination.get("window_start") else start
        if account_idx >= len(accounts) or window_start > end:
            return SourceIteration(records=[], next_pagination={})
        account_id = accounts[account_idx]
        window_end = min(end, window_start + timedelta(days=self.config.window_days - 1))

        request_id = self._submit_report(report_type, columns, account_id, window_start, window_end)
        url = self._poll_report(request_id, account_id)
        rows = self._download_csv(url) if url else []

        records = []
        for row in rows:
            data = {
                **row,
                "_account_id": account_id,
                "_window_start": window_start.isoformat(),
                "_window_end": window_end.isoformat(),
            }
            # Report rows have no id; every column is grain or measure, so the whole row is hashed.
            row_key = json.dumps([stream, account_id, row], sort_keys=True)
            records.append(SourceRecord(id=hashlib.sha1(row_key.encode()).hexdigest(), data=data))

        if window_end < end:
            next_pagination = {"account_idx": account_idx, "window_start": (window_end + timedelta(days=1)).isoformat()}
        elif account_idx + 1 < len(accounts):
            next_pagination = {"account_idx": account_idx + 1}
        else:
            next_pagination = {}
        logger.info(f"{stream}: {len(records)} rows for {account_id} {window_start}..{window_end}")
        return SourceIteration(records=records, next_pagination=next_pagination)

    def _start_date(self) -> Optional[date]:
        return date.fromisoformat(self.config.start_date) if self.config.start_date else None

    def _dispatch(self, report_start: Optional[date], pagination: Optional[dict]) -> SourceIteration:
        stream = self.config.stream.value
        if stream == "accounts":
            return self._get_accounts()
        if stream == "bulk_entities":
            return self._get_bulk_entities(pagination)
        return self._get_report(report_start, pagination)

    def get(self, pagination: dict = None) -> SourceIteration:
        return self._dispatch(self._start_date(), pagination)

    def get_records_after(self, source_state: SourceIncrementalState, pagination: dict = None) -> SourceIteration:
        start = source_state.last_run.date() - timedelta(days=self.config.lookback_days)
        if self._start_date():
            start = max(self._start_date(), start)
        return self._dispatch(start, pagination)


class BingApiError(Exception):
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self.payload = payload
        super().__init__(f"Microsoft Advertising API error {status_code}: {json.dumps(payload)[:1500]}")

    def invalid_column_index(self) -> Optional[int]:
        """Index of the column the API rejected, from a `$.Columns[i]` path in its error."""
        for error in self.payload.get("OperationErrors") or []:
            match = re.search(r"\$\.ReportRequest\.Columns\[(\d+)\]|\$\.Columns\[(\d+)\]", str(error.get("Details")))
            if match:
                return int(match.group(1) or match.group(2))
        return None
