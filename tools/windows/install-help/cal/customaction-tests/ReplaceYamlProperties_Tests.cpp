#include "stdafx.h"
#include "ReplaceYamlProperties.h"
#include <optional>

property_retriever propertyRetriever(value_map const &values)
{
    return [values](std::wstring const &propertyName) -> std::optional<std::wstring>
    {
        auto it = values.find(propertyName);
        if (it != values.end())
        {
            return it->second;
        }
        return std::nullopt;
    };
}

TEST_F(ReplaceYamlPropertiesTests, When_APIKEY_Present_Replace_It)
{
    value_map values = {{L"APIKEY", L"1234567890"}};
    std::wstring result = replace_yaml_properties(
LR"(
## @param api_key - string - required
## The Datadog API key to associate your Agent's data with your organization.
## Create a new API key here: https://app.datadoghq.com/account/settings
#
api_key:)",
                                                  propertyRetriever(values));

    EXPECT_EQ(result,
LR"(
## @param api_key - string - required
## The Datadog API key to associate your Agent's data with your organization.
## Create a new API key here: https://app.datadoghq.com/account/settings
#
api_key: 1234567890)");
}

TEST_F(ReplaceYamlPropertiesTests, When_EC2_USE_WINDOWS_PREFIX_DETECTION_Add_It)
{
    value_map values = {{L"EC2_USE_WINDOWS_PREFIX_DETECTION", L"true"}};
    std::wstring result = replace_yaml_properties(LR"()",
                                                  propertyRetriever(values));
    
    EXPECT_EQ(result, LR"(
ec2_use_windows_prefix_detection: true
)");
}

TEST_F(ReplaceYamlPropertiesTests, When_EC2_USE_WINDOWS_PREFIX_DETECTION_Already_Exists_Dont_Duplicate_it)
{
    value_map values = {{L"EC2_USE_WINDOWS_PREFIX_DETECTION", L"true"}};
    std::wstring result = replace_yaml_properties(LR"(
ec2_use_windows_prefix_detection: false
)",
                                                  propertyRetriever(values));

    EXPECT_EQ(result, LR"(
ec2_use_windows_prefix_detection: true
)");
}
