"""
Enterprise Web Scraping Framework - Parser Module Implementation
"""
from abc import ABC
import json
import logging
import re
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Union, Callable, Type
import traceback

from bs4 import BeautifulSoup
from jsonschema import validate, ValidationError
from lxml import etree
import cssselect
import jsonpath_ng

from core_framework import (
    ParserInterface,
    ScrapedResponse,
    ParsedData,
    Link
)


class DataExtractor(ABC):
    """Base class for all data extraction strategies"""

    def extract(self, content: Any, query: str, context: Dict[str, Any] = None) -> Any:
        """Extract data using the specific strategy"""
        raise NotImplementedError()


class DomExtractor(DataExtractor):
    """Extract data using BeautifulSoup and DOM traversal"""

    def __init__(self):
        self.logger = logging.getLogger("parser.dom")

    def extract(self, content: Any, query: str, context: Dict[str, Any] = None) -> Any:
        """Extract data using BeautifulSoup selectors"""
        context = context or {}
        soup = content if isinstance(content, BeautifulSoup) else BeautifulSoup(content, "html.parser")

        try:
            if ":" in query:
                selector_type, selector = query.split(":", 1)

                if selector_type == "text":
                    elements = soup.select(selector)
                    return " ".join(element.get_text(strip=True) for element in elements)

                elif selector_type == "html":
                    elements = soup.select(selector)
                    return " ".join(str(element) for element in elements)

                elif selector_type == "attr":
                    attr_name, selector = selector.split(",", 1)
                    elements = soup.select(selector)
                    return [element.get(attr_name) for element in elements if element.has_attr(attr_name)]

                elif selector_type == "exists":
                    return len(soup.select(selector)) > 0

                else:
                    self.logger.warning(f"Unknown selector type: {selector_type}")
                    return None
            else:
                elements = soup.select(query)
                if not elements:
                    return None

                # Single element vs list based on context
                if context.get("multiple", False):
                    return elements
                else:
                    return elements[0]

        except Exception as e:
            self.logger.error(f"Error in DOM extraction: {str(e)}")
            return None


class XPathExtractor(DataExtractor):
    """Extract data using XPath queries"""

    def __init__(self):
        self.logger = logging.getLogger("parser.xpath")

    def extract(self, content: Any, query: str, context: Dict[str, Any] = None) -> Any:
        """Extract data using XPath expressions"""
        context = context or {}

        try:
            if isinstance(content, str):
                tree = etree.HTML(content)
            elif isinstance(content, bytes):
                tree = etree.HTML(content.decode('utf-8', errors='replace'))
            else:
                tree = content

            result = tree.xpath(query)

            # Process results based on type
            if not result:
                return None

            if isinstance(result, list):
                if len(result) == 1 and not context.get("multiple", False):
                    result = result[0]

                # Convert lxml elements to strings if they're text nodes
                if isinstance(result, list):
                    result = [
                        (item.text if hasattr(item, 'text') and not isinstance(item, str) else item)
                        for item in result
                    ]

            return result

        except Exception as e:
            self.logger.error(f"Error in XPath extraction: {str(e)}\n{traceback.format_exc()}")
            return None


class CssSelectorExtractor(DataExtractor):
    """Extract data using CSS selectors with lxml for performance"""

    def __init__(self):
        self.logger = logging.getLogger("parser.css")
        self.parser = etree.HTMLParser()

    def extract(self, content: Any, query: str, context: Dict[str, Any] = None) -> Any:
        """Extract data using CSS selectors"""
        context = context or {}

        try:
            if isinstance(content, str):
                tree = etree.HTML(content, self.parser)
            elif isinstance(content, bytes):
                tree = etree.HTML(content.decode('utf-8', errors='replace'), self.parser)
            else:
                tree = content

            # Convert CSS to XPath for lxml
            css_translator = cssselect.HTMLTranslator()
            xpath_query = css_translator.css_to_xpath(query)

            # Get elements
            elements = tree.xpath(xpath_query)

            if not elements:
                return None

            # Process based on output_type in context
            output_type = context.get("output_type", "element")

            if output_type == "text":
                result = [etree.tostring(el, method='text', encoding='unicode').strip() for el in elements]
            elif output_type == "html":
                result = [etree.tostring(el, encoding='unicode') for el in elements]
            elif output_type == "attr" and "attr_name" in context:
                result = [el.get(context["attr_name"]) for el in elements if el.get(context["attr_name"]) is not None]
            else:
                result = elements

            # Return as list or single item
            if context.get("multiple", False):
                return result
            else:
                return result[0] if result else None

        except Exception as e:
            self.logger.error(f"Error in CSS extraction: {str(e)}")
            return None


class RegexExtractor(DataExtractor):
    """Extract data using regular expressions"""

    def __init__(self):
        self.logger = logging.getLogger("parser.regex")
        self.regex_cache = {}

    def extract(self, content: Any, query: str, context: Dict[str, Any] = None) -> Any:
        """Extract data using regex patterns"""
        context = context or {}

        try:
            if isinstance(content, bytes):
                content = content.decode('utf-8', errors='replace')
            elif not isinstance(content, str):
                content = str(content)

            # Use cached compiled regex if available
            if query not in self.regex_cache:
                self.regex_cache[query] = re.compile(query, re.DOTALL)

            pattern = self.regex_cache[query]

            if context.get("findall", False):
                matches = pattern.findall(content)

                # Handle capturing groups
                if matches and isinstance(matches[0], tuple):
                    group_idx = context.get("group", 0)
                    if group_idx == "all":
                        return matches
                    else:
                        return [match[group_idx] if len(match) > group_idx else match for match in matches]
                return matches
            else:
                match = pattern.search(content)
                if not match:
                    return None

                # Return specific capturing group or entire match
                group_idx = context.get("group", 0)
                if group_idx == "all":
                    return match.groups()
                else:
                    try:
                        return match.group(group_idx)
                    except IndexError:
                        return match.group(0)

        except Exception as e:
            self.logger.error(f"Error in regex extraction: {str(e)}")
            return None


class JsonExtractor(DataExtractor):
    """Extract data from JSON using JSONPath"""

    def __init__(self):
        self.logger = logging.getLogger("parser.json")
        self.jsonpath_cache = {}

    def extract(self, content: Any, query: str, context: Dict[str, Any] = None) -> Any:
        """Extract data using JSONPath expressions"""
        context = context or {}

        try:
            # Parse JSON if needed
            if isinstance(content, (str, bytes)):
                if isinstance(content, bytes):
                    content = content.decode('utf-8', errors='replace')
                try:
                    json_data = json.loads(content)
                except json.JSONDecodeError:
                    # Try to extract JSON from HTML
                    match = re.search(r'<script[^>]*type=["\'](application|text)/json["\'][^>]*>(.*?)</script>',
                                    content, re.DOTALL)
                    if match:
                        try:
                            json_data = json.loads(match.group(2))
                        except json.JSONDecodeError:
                            self.logger.error("Failed to parse JSON from embedded script")
                            return None
                    else:
                        self.logger.error("Content is not valid JSON and no JSON script found")
                        return None
            else:
                json_data = content

            # Use cached JSONPath expression if available
            if query not in self.jsonpath_cache:
                self.jsonpath_cache[query] = jsonpath_ng.parse(query)

            jsonpath_expr = self.jsonpath_cache[query]
            matches = [match.value for match in jsonpath_expr.find(json_data)]

            if not matches:
                return None

            # Return as list or single item
            if context.get("multiple", False):
                return matches
            else:
                return matches[0] if matches else None

        except Exception as e:
            self.logger.error(f"Error in JSON extraction: {str(e)}")
            return None


@dataclass
class ExtractionField:
    """Defines a field to be extracted from content"""
    name: str
    extractor_type: str
    query: str
    required: bool = False
    default: Any = None
    multiple: bool = False
    context: Dict[str, Any] = field(default_factory=dict)
    transform: Optional[Callable[[Any], Any]] = None
    children: List['ExtractionField'] = field(default_factory=list)


@dataclass
class ExtractionSchema:
    """Complete schema for extracting structured data"""
    name: str
    version: str
    fields: List[ExtractionField]
    validators: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)


class Parser(ParserInterface):
    """Implementation of the Parser Interface"""

    def __init__(self):
        self.schemas: Dict[str, ExtractionSchema] = {}
        self.extractors: Dict[str, DataExtractor] = {
            "dom": DomExtractor(),
            "xpath": XPathExtractor(),
            "css": CssSelectorExtractor(),
            "regex": RegexExtractor(),
            "json": JsonExtractor()
        }
        self.logger = logging.getLogger("parser")
        self.transformer_registry: Dict[str, Callable] = self._register_default_transformers()

    def _register_default_transformers(self) -> Dict[str, Callable]:
        """Register built-in data transformation functions"""
        return {
            "strip": lambda x: x.strip() if isinstance(x, str) else x,
            "lowercase": lambda x: x.lower() if isinstance(x, str) else x,
            "uppercase": lambda x: x.upper() if isinstance(x, str) else x,
            "int": lambda x: int(x) if x is not None and x != "" else None,
            "float": lambda x: float(x) if x is not None and x != "" else None,
            "bool": lambda x: bool(x) if x is not None else None,
            "join": lambda x, sep=" ": sep.join(x) if isinstance(x, list) else x,
            "first": lambda x: x[0] if isinstance(x, list) and x else x,
            "last": lambda x: x[-1] if isinstance(x, list) and x else x,
            "length": lambda x: len(x) if x is not None else 0
        }

    def register_transformer(self, name: str, func: Callable) -> None:
        """Register a custom data transformation function"""
        self.transformer_registry[name] = func
        self.logger.info(f"Registered custom transformer: {name}")

    def register_schema(self, schema_name: str, schema_definition: Dict[str, Any]) -> None:
        """Register a new extraction schema"""
        try:
            # Process schema definition
            fields = []
            for field_def in schema_definition.get("fields", []):
                transform_funcs = None
                if "transform" in field_def:
                    if isinstance(field_def["transform"], str):
                        # Single transform function
                        transform_name = field_def["transform"]
                        if transform_name in self.transformer_registry:
                            transform_funcs = self.transformer_registry[transform_name]
                    elif isinstance(field_def["transform"], list):
                        # Chain of transform functions
                        transforms = []
                        for t in field_def["transform"]:
                            if isinstance(t, str) and t in self.transformer_registry:
                                transforms.append(self.transformer_registry[t])

                        if transforms:
                            # Create a chained transform function
                            def chain_transform(value, funcs=transforms):
                                for func in funcs:
                                    value = func(value)
                                return value
                            transform_funcs = chain_transform

                # Process child fields for nested data
                children = []
                if "children" in field_def:
                    for child_def in field_def.get("children", []):
                        child_transform = None
                        if "transform" in child_def and child_def["transform"] in self.transformer_registry:
                            child_transform = self.transformer_registry[child_def["transform"]]

                        children.append(ExtractionField(
                            name=child_def["name"],
                            extractor_type=child_def["extractor"],
                            query=child_def["query"],
                            required=child_def.get("required", False),
                            default=child_def.get("default"),
                            multiple=child_def.get("multiple", False),
                            context=child_def.get("context", {}),
                            transform=child_transform
                        ))

                # Create field
                fields.append(ExtractionField(
                    name=field_def["name"],
                    extractor_type=field_def["extractor"],
                    query=field_def["query"],
                    required=field_def.get("required", False),
                    default=field_def.get("default"),
                    multiple=field_def.get("multiple", False),
                    context=field_def.get("context", {}),
                    transform=transform_funcs,
                    children=children
                ))

            # Create and store schema
            schema = ExtractionSchema(
                name=schema_definition["name"],
                version=schema_definition.get("version", "1.0"),
                fields=fields,
                validators=schema_definition.get("validators", []),
                metadata=schema_definition.get("metadata", {})
            )

            self.schemas[schema_name] = schema
            self.logger.info(f"Registered schema: {schema_name} (v{schema.version})")

        except Exception as e:
            self.logger.error(f"Error registering schema {schema_name}: {str(e)}\n{traceback.format_exc()}")
            raise ValueError(f"Invalid schema definition: {str(e)}")

    def parse(self, response: ScrapedResponse, schema_name: str) -> ParsedData:
        """Extract structured data from a response using a named schema"""
        if schema_name not in self.schemas:
            raise ValueError(f"Schema not found: {schema_name}")

        schema = self.schemas[schema_name]
        start_time = time.time()

        try:
            # Prepare content based on content type
            content_type = response.content_type.lower()

            if "json" in content_type:
                primary_content = json.loads(response.content.decode("utf-8", errors="replace"))
            elif "html" in content_type or "xml" in content_type:
                primary_content = response.content
            else:
                # Default to raw content
                primary_content = response.content

            # Extract data using schema
            data = self._extract_data(primary_content, schema)

            # Validate extracted data
            validated = self._validate_data(data, schema)

            extraction_time = time.time() - start_time
            self.logger.info(f"Parsed {response.task.url} with schema {schema_name} in {extraction_time:.2f}s")

            return ParsedData(
                response=response,
                data=data,
                schema_name=schema_name,
                validated=validated,
                extraction_time=extraction_time
            )

        except Exception as e:
            self.logger.error(f"Error parsing {response.task.url} with schema {schema_name}: {str(e)}\n{traceback.format_exc()}")
            raise

    def _extract_data(self, content: Any, schema: ExtractionSchema) -> Dict[str, Any]:
        """Extract data according to schema fields"""
        result = {}

        for field in schema.fields:
            try:
                if field.extractor_type not in self.extractors:
                    self.logger.warning(f"Unknown extractor: {field.extractor_type}")
                    continue

                extractor = self.extractors[field.extractor_type]
                context = field.context.copy()
                context["multiple"] = field.multiple

                # Extract field value
                value = extractor.extract(content, field.query, context)

                # Apply transformation if specified
                if field.transform and value is not None:
                    value = field.transform(value)

                # Extract child fields for nested data structures
                if field.children and value is not None:
                    if field.multiple and isinstance(value, list):
                        # Handle extracting from a list of elements
                        nested_values = []
                        for item in value:
                            nested_item = {}
                            for child in field.children:
                                child_context = child.context.copy()
                                child_context["multiple"] = child.multiple
                                child_extractor = self.extractors[child.extractor_type]
                                child_value = child_extractor.extract(item, child.query, child_context)

                                if child.transform and child_value is not None:
                                    child_value = child.transform(child_value)

                                if child_value is not None or child.required:
                                    nested_item[child.name] = child_value if child_value is not None else child.default

                            if nested_item:
                                nested_values.append(nested_item)

                        value = nested_values
                    else:
                        # Handle extracting from a single element
                        nested_value = {}
                        for child in field.children:
                            child_context = child.context.copy()
                            child_context["multiple"] = child.multiple
                            child_extractor = self.extractors[child.extractor_type]
                            child_value = child_extractor.extract(value, child.query, child_context)

                            if child.transform and child_value is not None:
                                child_value = child.transform(child_value)

                            if child_value is not None or child.required:
                                nested_value[child.name] = child_value if child_value is not None else child.default

                        value = nested_value

                # Use default value for required fields if extraction returns None
                if value is None:
                    if field.required:
                        result[field.name] = field.default
                else:
                    result[field.name] = value

            except Exception as e:
                self.logger.error(f"Error extracting field {field.name}: {str(e)}")
                if field.required:
                    result[field.name] = field.default

        return result

    def _validate_data(self, data: Dict[str, Any], schema: ExtractionSchema) -> bool:
        """Validate extracted data against schema validators"""
        if not schema.validators:
            return True

        for validator in schema.validators:
            validator_type = validator.get("type")

            if validator_type == "json_schema":
                try:
                    json_schema = validator.get("schema", {})
                    validate(instance=data, schema=json_schema)
                except ValidationError as e:
                    self.logger.warning(f"JSON Schema validation failed: {str(e)}")
                    return False

            elif validator_type == "custom" and "function" in validator:
                try:
                    # Run custom validation function (not implemented here - would require eval or importlib)
                    pass
                except Exception as e:
                    self.logger.warning(f"Custom validation failed: {str(e)}")
                    return False

            elif validator_type == "required_fields":
                required_fields = validator.get("fields", [])
                for field in required_fields:
                    if field not in data or data[field] is None:
                        self.logger.warning(f"Required field missing: {field}")
                        return False

        return True

    def extract_links(self, response: ScrapedResponse) -> List[Link]:
        """Extract links from a response for further crawling"""
        links = []

        # Only extract links from HTML content
        if not response.content_type or "html" not in response.content_type.lower():
            return links

        try:
            # Parse HTML
            content = response.content.decode("utf-8", errors="replace")
            soup = BeautifulSoup(content, "html.parser")

            # Find all anchor tags
            for anchor in soup.find_all("a", href=True):
                href = anchor.get("href", "").strip()

                # Skip empty, javascript, and mailto links
                if not href or href.startswith(("javascript:", "mailto:", "tel:")):
                    continue

                # Check for nofollow
                rel = anchor.get("rel", [])
                nofollow = isinstance(rel, list) and "nofollow" in rel

                # Create link object
                link = Link(
                    url=href,
                    source_url=response.task.url,
                    text=anchor.get_text(strip=True) or None,
                    nofollow=nofollow,
                    depth=response.task.depth + 1
                )

                links.append(link)

            # Also extract links from other elements that might contain URLs
            link_elements = {
                "img": "src",
                "script": "src",
                "link": "href",
                "iframe": "src"
            }

            for tag, attr in link_elements.items():
                for element in soup.find_all(tag):
                    if element.has_attr(attr):
                        url = element.get(attr, "").strip()
                        if url and not url.startswith(("javascript:", "data:", "#")):
                            link = Link(
                                url=url,
                                source_url=response.task.url,
                                text=None,
                                nofollow=True,  # Non-anchor links are typically not for crawling
                                depth=response.task.depth + 1
                            )
                            links.append(link)

            self.logger.info(f"Extracted {len(links)} links from {response.task.url}")
            return links

        except Exception as e:
            self.logger.error(f"Error extracting links from {response.task.url}: {str(e)}")
            return links
