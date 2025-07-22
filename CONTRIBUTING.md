# Contributing to PostgreSQL MCP Server

Thank you for your interest in contributing to the PostgreSQL MCP Server! We welcome contributions from the community.

## Code of Conduct

By participating in this project, you agree to abide by our code of conduct:
- Be respectful and inclusive
- Focus on constructive feedback
- Help maintain a welcoming environment for all contributors

## How to Contribute

### Reporting Issues

1. **Search existing issues** first to avoid duplicates
2. **Use the issue templates** when available
3. **Provide detailed information**:
   - Clear description of the problem
   - Steps to reproduce
   - Expected vs actual behavior
   - Environment details (OS, Python version, PostgreSQL version)

### Security Issues

**⚠️ NEVER report security vulnerabilities through public GitHub issues!**

Please follow our [Security Policy](SECURITY.md) for responsible disclosure.

### Pull Requests

1. **Fork the repository** and create a feature branch
2. **Follow coding standards**:
   - Use meaningful variable and function names
   - Add docstrings for new functions and classes
   - Follow PEP 8 style guidelines
   - Add type hints where appropriate

3. **Security Requirements**:
   - Never commit credentials or secrets
   - Use parameterized queries for all database operations
   - Validate all user inputs
   - Follow principle of least privilege
   - Add security tests for new features

4. **Testing Requirements**:
   - Add tests for new functionality
   - Ensure all existing tests pass
   - Test with different PostgreSQL versions
   - Include integration tests where appropriate

5. **Documentation**:
   - Update README if needed
   - Add docstrings for new functions
   - Update tool documentation
   - Include usage examples

### Development Setup

1. **Clone your fork**:
   ```bash
   git clone https://github.com/yourusername/postgres-mcp-server.git
   cd postgres-mcp-server
   ```

2. **Create virtual environment**:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   pip install -r requirements-dev.txt  # If available
   ```

4. **Set up test database**:
   ```bash
   cp .env.example .env
   # Edit .env with your test database credentials
   ```

5. **Run tests**:
   ```bash
   python -m pytest tests/
   ```

### Code Review Process

1. All submissions require code review
2. We use GitHub's review features
3. Address feedback promptly
4. Maintain a clean commit history
5. Squash commits before merging if requested

### Security Code Review Checklist

- [ ] No hardcoded credentials or secrets
- [ ] SQL injection prevention (parameterized queries)
- [ ] Input validation and sanitization  
- [ ] Error handling doesn't leak sensitive information
- [ ] Proper authentication and authorization
- [ ] Secure logging practices
- [ ] Dependencies are up to date and secure

## Development Guidelines

### Database Tool Development

When adding new PostgreSQL tools:

1. **Follow naming convention**: `PostgreSQL_action_description`
2. **Include proper error handling**
3. **Use type hints and docstrings**
4. **Add parameter validation**
5. **Test with different PostgreSQL versions**
6. **Consider performance implications**

### Example Tool Structure

```python
async def PostgreSQL_example_tool(
    connection_string: str,
    parameter: str,
    optional_param: Optional[str] = None
) -> dict:
    """
    Brief description of what the tool does.
    
    Args:
        connection_string: PostgreSQL connection string
        parameter: Description of required parameter
        optional_param: Description of optional parameter
        
    Returns:
        Dictionary containing tool results
        
    Raises:
        ConnectionError: If database connection fails
        ValueError: If parameters are invalid
    """
    # Implementation here
```

## Release Process

1. Version bumping follows semantic versioning (MAJOR.MINOR.PATCH)
2. Security fixes may trigger immediate patch releases
3. All releases are tagged and documented
4. Release notes include security advisories if applicable

## Getting Help

- **GitHub Discussions**: For general questions and ideas
- **Issues**: For bug reports and feature requests
- **Security**: Follow our security policy for vulnerabilities

## Recognition

Contributors will be acknowledged in:
- Release notes
- Repository contributors section
- Special recognition for security researchers

Thank you for helping make PostgreSQL MCP Server better and more secure!
