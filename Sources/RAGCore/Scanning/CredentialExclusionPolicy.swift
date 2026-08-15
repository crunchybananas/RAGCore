//
//  CredentialExclusionPolicy.swift
//  RAGCore
//
//  The mandatory credential boundary (#11): files matching these patterns are
//  never read, chunked, stored, or embedded — regardless of .ragignore.
//  Post-index scrubbing is not a boundary; by then the bytes have been read,
//  stored, and possibly served to a search or shared to a peer.
//

import Foundation

/// Mandatory, always-on exclusion of credential-shaped files from ingestion.
///
/// Three properties define the boundary:
///
/// 1. **It runs before any byte is read.** The scanner consults it ahead of
///    every other filter, so an excluded file never reaches `loadFile`.
/// 2. **`.ragignore` cannot weaken it.** The policy is checked independently
///    of the ignore machinery; no ignore pattern, present or absent, negated
///    or not, re-includes a credential file. The only way through is the
///    explicit `unsafeAllowPatterns` opt-in, which exists so a repository of
///    deliberate FIXTURE credentials (test keys, sample service accounts)
///    can be indexed — and which every caller should treat as a visible,
///    logged decision.
/// 3. **It is name-based and conservative.** Patterns match well-known
///    credential file shapes; they do not sniff contents (the file is never
///    read, so they cannot). A miss here is caught by nothing, so the list
///    prefers false positives (an excluded lookalike) over false negatives.
public struct CredentialExclusionPolicy: Sendable, Equatable {

  /// The mandatory pattern set. fnmatch(3) syntax, matched case-insensitively
  /// against BOTH the file name and the root-relative path (same mechanics as
  /// `.ragignore` patterns, so behavior is uniform).
  public static let mandatoryPatterns: [String] = [
    // Private keys, certificates, signing material
    "*.pem", "*.key", "*.p12", "*.pfx", "*.keystore", "*.jks", "*.ppk",
    "id_rsa*", "id_ed25519*", "id_ecdsa*", "id_dsa*",
    "*.mobileprovision", "*.provisionprofile",
    // Environment files — the `env` extension is in the scanner's allowlist,
    // so prod.env / staging.env were read and indexed before this policy.
    // Dotfiles (.env, .env.local) are additionally dropped by the hidden-file
    // rule, but the policy names them so the guarantee does not depend on a
    // traversal option.
    ".env", ".env.*", "*.env",
    // Service accounts, OAuth, platform credential bundles
    "*service-account*.json", "*service_account*.json",
    "client_secret*.json", "credentials.json",
    "google-services.json", "GoogleService-Info.plist",
    // Secret stores and access files
    "secrets.yaml", "secrets.yml", "secrets.json", "*.secrets.*",
    "*.tfvars", ".netrc", ".npmrc", ".pgpass", ".htpasswd", "*.kdbx",
  ]

  /// Patterns the caller has deliberately re-allowed. Each entry must match
  /// one of `mandatoryPatterns` EXACTLY — the opt-out is per named pattern,
  /// never a free-form re-include, so a caller can state "this repository's
  /// *.pem files are deliberate fixtures" without silently disabling the
  /// whole boundary.
  public let unsafeAllowPatterns: Set<String>

  private let activePatterns: [String]

  public init(unsafeAllowPatterns: Set<String> = []) {
    self.unsafeAllowPatterns = unsafeAllowPatterns
    self.activePatterns = Self.mandatoryPatterns.filter { !unsafeAllowPatterns.contains($0) }
  }

  /// The always-on default: every mandatory pattern active.
  public static let standard = CredentialExclusionPolicy()

  /// Whether ingestion must refuse this file. `relativePath` is the
  /// root-relative path with `/` separators; `fileName` its last component.
  /// For symlinks, callers pass the link name AND, when resolvable, should
  /// also check the destination's name — a link named `config.txt` pointing
  /// at `id_rsa` is still a credential read.
  public func excludes(relativePath: String, fileName: String) -> Bool {
    let loweredPath = relativePath.lowercased()
    let loweredName = fileName.lowercased()
    for pattern in activePatterns {
      let loweredPattern = pattern.lowercased()
      if fnmatch(loweredPattern, loweredName, 0) == 0 { return true }
      if fnmatch(loweredPattern, loweredPath, 0) == 0 { return true }
    }
    return false
  }
}
