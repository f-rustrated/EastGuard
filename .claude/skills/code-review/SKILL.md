---
name: code-review
metadata:
  version: 1.0.0
description: Code review guidelines. Use when reviewing a PR, performing code review, or checking code quality
---

# Code Review Guidelines

All AI agents performing code reviews must follow these guidelines to ensure consistent, valuable reviews.

## Core Review Principles

- **Be Objective**: Base feedback on objective criteria from these guidelines. When describing changes or issues, focus on the code's behavior and its impact. Avoid subjective, unconstructive labels like "good" or "bad."
- **Use Code as Source of Truth**: Base all summaries on the code diff. Do not trust or rephrase the PR description, which may be outdated or inaccurate. A summary must reflect the actual changes in the code.
- **Be Concise**: Generate summaries that are brief and to the point. Focus on the most significant changes, and avoid unnecessary details or verbose explanations.

## Code Quality Focus Areas

- Check for adherence to existing project patterns and conventions
- Verify proper error handling and validation implementation
- Ensure meaningful variable and function names are used
- Validate appropriate test coverage for new functionality
- Assess code readability and maintainability
- Review for potential performance implications

## Security Considerations

- Flag potential security vulnerabilities or weaknesses
- Verify proper input validation and sanitization
- Assess data handling and storage security

## Documentation and Maintainability

- Ensure code changes include necessary documentation/roadmap updates
- Check for clear, self-documenting code structure
- Verify consistent formatting and style adherence
- Assess impact on overall codebase maintainability
- Review for appropriate inline comments and explanations


## Testing Considerations

- Property testing based on assert_invariants for any unit testing
- Oracle testing based on unittest
- Prefer blackbox testing on integration testing

## Review Comment Style

### Severity Classification

- **Critical**: Security issues, breaking changes, major bugs that prevent functionality
- **High**: Performance issues, significant design concerns, important bugs
- **Medium**: Code quality improvements, minor bugs, maintainability concerns
- **Low**: Style suggestions, minor optimizations, nitpicks

## Review Scope Guidelines

- Focus primarily on modified files and their direct impact
- Consider broader system implications and architectural effects
- Check for proper file organization and project structure adherence
- Verify changes align with established project patterns
- Assess the scope and complexity of changes for reviewability

## Change Impact Assessment

- Evaluate whether changes follow the "smaller changes philosophy" and project roadmap
- Consider the impact on existing functionality and dependencies
- Review for potential breaking changes and backward compatibility
- Assess the necessity and appropriateness of the proposed changes
- Validate that changes solve the intended problem effectively

