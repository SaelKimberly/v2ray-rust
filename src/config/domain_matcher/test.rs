use super::ac_automaton::ACAutomaton;
use super::ac_automaton::HybridMatcher;
use super::mph::MphMatcher;
use super::{DomainMatcher, MatchType};

#[cfg(test)]
fn test_domain_matcher(matcher: &mut impl DomainMatcher) {
    matcher.reverse_insert("163.com", MatchType::Domain(true));
    matcher.reverse_insert("m.126.com", MatchType::Full(true));
    matcher.reverse_insert("3.com", MatchType::Full(true));
    matcher.reverse_insert("google.com", MatchType::SubStr(true));
    matcher.reverse_insert("vgoogle.com", MatchType::SubStr(true));
    matcher.build();
    assert_eq!(matcher.reverse_query("126.com"), false);
    assert_eq!(matcher.reverse_query("mm163.com"), false);
    assert_eq!(matcher.reverse_query("m.163.com"), true); // sub domain
    assert_eq!(matcher.reverse_query("163.com"), true); // sub domain
    assert_eq!(matcher.reverse_query("63.com"), false);
    assert_eq!(matcher.reverse_query("m.126.com"), true); // full match
    assert_eq!(matcher.reverse_query("oogle.com"), false);
    assert_eq!(matcher.reverse_query("vvgoogle.com"), true); // substr
    matcher.clear();
    matcher.reverse_insert("video.google.com", MatchType::Domain(true));
    matcher.reverse_insert("gle.com", MatchType::Domain(true));
    matcher.build();
    assert_eq!(matcher.reverse_query("google.com"), false);
    assert_eq!(matcher.reverse_query("video.google.com.hk"), false); // not sub domain
}

#[test]
fn test_ac_automaton() {
    let mut ac_automaton = ACAutomaton::new(1);
    test_domain_matcher(&mut ac_automaton);
}

#[test]
fn test_hybrid_matcher() {
    let mut hybrid_matcher = HybridMatcher::new(1);
    test_domain_matcher(&mut hybrid_matcher);
}

#[test]
fn test_mph_matcher() {
    let mut mph_matcher = MphMatcher::new(1);
    test_domain_matcher(&mut mph_matcher);
}
