//! License types and tiers.

use serde::{Deserialize, Serialize};

/// License tier levels.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Tier {
    /// Free tier (no license key required)
    /// Company revenue < $1M/year
    #[default]
    Free,

    /// Pro tier ($299/mo)
    /// Company revenue $1M-$10M/year
    Pro,

    /// Enterprise tier ($2000+/mo)
    /// Company revenue > $10M/year
    Enterprise,
}

impl Tier {
    /// Get display name for the tier.
    pub fn display_name(&self) -> &'static str {
        match self {
            Self::Free => "Tell",
            Self::Pro => "Tell Pro",
            Self::Enterprise => "Tell Enterprise",
        }
    }

    /// Check if this tier requires a license key.
    pub fn requires_license(&self) -> bool {
        !matches!(self, Self::Free)
    }

    /// Parse tier from string.
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "free" => Some(Self::Free),
            "pro" => Some(Self::Pro),
            "enterprise" => Some(Self::Enterprise),
            _ => None,
        }
    }
}

impl std::fmt::Display for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name())
    }
}

/// License payload embedded in the key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LicensePayload {
    /// Customer identifier (company name or ID)
    pub customer_id: String,

    /// Customer display name
    pub customer_name: String,

    /// License tier
    pub tier: Tier,

    /// Expiry date (ISO 8601 date: YYYY-MM-DD)
    pub expires: String,

    /// Issue date (ISO 8601 date: YYYY-MM-DD)
    pub issued: String,
}

impl LicensePayload {
    /// Check if the license has expired.
    pub fn is_expired(&self) -> bool {
        use chrono::NaiveDate;

        let Ok(expiry) = NaiveDate::parse_from_str(&self.expires, "%Y-%m-%d") else {
            return true; // Invalid date = expired
        };

        let today = chrono::Utc::now().date_naive();
        today > expiry
    }

    /// Get days until expiry (negative if expired).
    pub fn days_until_expiry(&self) -> i64 {
        use chrono::NaiveDate;

        let Ok(expiry) = NaiveDate::parse_from_str(&self.expires, "%Y-%m-%d") else {
            return -1;
        };

        let today = chrono::Utc::now().date_naive();
        (expiry - today).num_days()
    }
}

/// Validated license information.
#[derive(Debug, Clone)]
pub struct License {
    /// The validated payload
    pub payload: LicensePayload,

    /// Raw key (for display/storage)
    pub raw_key: String,
}

impl License {
    /// Get the tier.
    pub fn tier(&self) -> Tier {
        self.payload.tier
    }

    /// Get customer display name.
    pub fn customer_name(&self) -> &str {
        &self.payload.customer_name
    }

    /// Check if expired.
    pub fn is_expired(&self) -> bool {
        self.payload.is_expired()
    }

    /// Check if license is valid (not expired).
    pub fn is_valid(&self) -> bool {
        !self.is_expired()
    }
}

/// Current license state for the application.
#[derive(Debug, Clone)]
pub enum LicenseState {
    /// No license key configured (free tier)
    Free,

    /// Valid license
    Licensed(License),

    /// License key exists but is expired
    Expired(License),

    /// License key exists but is invalid (bad signature, malformed)
    Invalid(String),
}

impl LicenseState {
    /// Get the effective tier (Free if not validly licensed).
    pub fn tier(&self) -> Tier {
        match self {
            Self::Free => Tier::Free,
            Self::Licensed(lic) => lic.tier(),
            Self::Expired(_) => Tier::Free, // Expired = no service
            Self::Invalid(_) => Tier::Free,
        }
    }

    /// Check if the server should run.
    /// Returns false if there's an expired Pro/Enterprise license.
    pub fn can_serve(&self) -> bool {
        match self {
            Self::Free => true,
            Self::Licensed(_) => true,
            Self::Expired(_) => false, // No pay, no serve
            Self::Invalid(_) => true,  // Invalid key = treat as free
        }
    }

    /// Get display string for TUI.
    pub fn display(&self) -> String {
        match self {
            Self::Free => "Tell".to_string(),
            Self::Licensed(lic) => {
                format!("{} ({})", lic.tier().display_name(), lic.customer_name())
            }
            Self::Expired(lic) => {
                format!(
                    "{} (EXPIRED - {})",
                    lic.tier().display_name(),
                    lic.customer_name()
                )
            }
            Self::Invalid(err) => format!("Tell (invalid license: {})", err),
        }
    }

    /// Check if licensed (Pro or Enterprise, not expired).
    pub fn is_licensed(&self) -> bool {
        matches!(self, Self::Licensed(_))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tier_display() {
        assert_eq!(Tier::Free.display_name(), "Tell");
        assert_eq!(Tier::Pro.display_name(), "Tell Pro");
        assert_eq!(Tier::Enterprise.display_name(), "Tell Enterprise");
    }

    #[test]
    fn test_tier_requires_license() {
        assert!(!Tier::Free.requires_license());
        assert!(Tier::Pro.requires_license());
        assert!(Tier::Enterprise.requires_license());
    }

    #[test]
    fn test_payload_expiry() {
        let future = LicensePayload {
            customer_id: "test".to_string(),
            customer_name: "Test Corp".to_string(),
            tier: Tier::Pro,
            expires: "2099-12-31".to_string(),
            issued: "2024-01-01".to_string(),
        };
        assert!(!future.is_expired());
        assert!(future.days_until_expiry() > 0);

        let past = LicensePayload {
            customer_id: "test".to_string(),
            customer_name: "Test Corp".to_string(),
            tier: Tier::Pro,
            expires: "2020-01-01".to_string(),
            issued: "2019-01-01".to_string(),
        };
        assert!(past.is_expired());
        assert!(past.days_until_expiry() < 0);
    }

    #[test]
    fn test_license_state_can_serve() {
        assert!(LicenseState::Free.can_serve());
        assert!(
            !LicenseState::Expired(License {
                payload: LicensePayload {
                    customer_id: "test".to_string(),
                    customer_name: "Test".to_string(),
                    tier: Tier::Pro,
                    expires: "2020-01-01".to_string(),
                    issued: "2019-01-01".to_string(),
                },
                raw_key: "test".to_string(),
            })
            .can_serve()
        );
    }
}
