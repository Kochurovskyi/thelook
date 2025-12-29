#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Command-line interface for e-commerce analytics agent.

Usage:
    # Interactive mode
    python cli.py

    # Single query
    python cli.py "Show me top 10 products by revenue"

    # Multiple queries from file
    python cli.py --file queries.txt

    # Verbose output
    python cli.py --verbose "Segment customers by order value"
"""
import sys
import os
import argparse
import json
from typing import Optional, List
from agents.graph import run_agent
from utils.request_context import RequestContext
from utils.logging_config import setup_logging_config
import pandas as pd

# Fix Unicode encoding on Windows
if sys.platform == 'win32':
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


def format_result(result: dict, verbose: bool = False) -> str:
    """
    Format agent result for display
    
    Args:
        result: Agent state result
        verbose: Whether to show detailed output
        
    Returns:
        Formatted string
    """
    output = []
    output.append("=" * 80)
    
    # Query
    if result.get("query"):
        output.append(f"Query: {result['query']}")
        output.append("-" * 80)
    
    # Error handling
    if result.get("error"):
        output.append(f"[ERROR] Error: {result['error']}")
        output.append("=" * 80)
        return "\n".join(output)
    
    # SQL Query
    if result.get("sql_query"):
        output.append("SQL Query:")
        output.append("-" * 80)
        sql = result["sql_query"]
        # Format SQL with basic indentation
        lines = sql.split("\n")
        formatted_sql = "\n".join(f"  {line}" for line in lines)
        output.append(formatted_sql)
        output.append("-" * 80)
    
    # Query Result
    if result.get("query_result") is not None:
        df = result["query_result"]
        if not df.empty:
            output.append(f"\nResults ({len(df)} rows, {len(df.columns)} columns):")
            output.append("-" * 80)
            # Display first 20 rows
            display_df = df.head(20)
            output.append(display_df.to_string(index=False))
            if len(df) > 20:
                output.append(f"\n... ({len(df) - 20} more rows)")
        else:
            output.append("\nResults: (empty)")
    
    # Insights
    if result.get("insights"):
        output.append("\n" + "-" * 80)
        output.append("Business Insights:")
        output.append("-" * 80)
        output.append(result["insights"])
    
    # Visualization info
    if result.get("visualization_spec"):
        viz = result["visualization_spec"]
        output.append("\n" + "-" * 80)
        output.append("Visualization:")
        output.append("-" * 80)
        output.append(f"Type: {viz.get('type', 'N/A')}")
        if viz.get("title"):
            output.append(f"Title: {viz['title']}")
    
    # Verbose information
    if verbose:
        output.append("\n" + "-" * 80)
        output.append("Verbose Information:")
        output.append("-" * 80)
        if result.get("query_metadata"):
            output.append(f"Query Metadata: {json.dumps(result['query_metadata'], indent=2)}")
        if result.get("request_id"):
            output.append(f"Request ID: {result.get('request_id')}")
    
    output.append("=" * 80)
    return "\n".join(output)


def run_interactive_mode(verbose: bool = False):
    """
    Run CLI in interactive mode
    
    Args:
        verbose: Whether to show verbose output
    """
    print("=" * 80)
    print("E-commerce Analytics Agent - Interactive Mode")
    print("=" * 80)
    print("Enter your queries (type 'exit' or 'quit' to exit, 'help' for help)")
    print("=" * 80)
    
    while True:
        try:
            query = input("\n> ").strip()
            
            if not query:
                continue
            
            if query.lower() in ['exit', 'quit', 'q']:
                print("\nGoodbye!")
                break
            
            if query.lower() == 'help':
                print("\nAvailable commands:")
                print("  exit, quit, q - Exit the CLI")
                print("  help - Show this help message")
                print("  clear - Clear screen (not implemented)")
                print("\nEnter natural language queries about the e-commerce data.")
                print("Examples:")
                print("  - Show me top 10 products by revenue")
                print("  - Segment customers by their total order value")
                print("  - What are the sales trends over the last 12 months?")
                continue
            
            # Process query
            print("\nProcessing query...")
            RequestContext.set_request_id(f"cli_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}")
            
            result = run_agent(query, use_specialized_agents=True)
            
            # Display result
            print("\n" + format_result(result, verbose=verbose))
            
        except KeyboardInterrupt:
            print("\n\nInterrupted. Type 'exit' to quit.")
        except EOFError:
            print("\n\nGoodbye!")
            break
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
            if verbose:
                import traceback
                traceback.print_exc()


def run_single_query(query: str, verbose: bool = False):
    """
    Run a single query
    
    Args:
        query: Natural language query
        verbose: Whether to show verbose output
    """
    RequestContext.set_request_id(f"cli_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}")
    
    result = run_agent(query, use_specialized_agents=True)
    
    print(format_result(result, verbose=verbose))


def run_batch_queries(queries: List[str], verbose: bool = False):
    """
    Run multiple queries
    
    Args:
        queries: List of natural language queries
        verbose: Whether to show verbose output
    """
    print("=" * 80)
    print(f"Running {len(queries)} queries...")
    print("=" * 80)
    
    results = []
    for i, query in enumerate(queries, 1):
        print(f"\n[{i}/{len(queries)}] Processing: {query}")
        RequestContext.set_request_id(f"cli_batch_{i}_{pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}")
        
        try:
            result = run_agent(query, use_specialized_agents=True)
            results.append({
                "query": query,
                "success": result.get("error") is None,
                "error": result.get("error"),
                "rows": len(result.get("query_result", [])) if result.get("query_result") is not None else 0
            })
            print(format_result(result, verbose=verbose))
        except Exception as e:
            print(f"❌ Error processing query: {str(e)}")
            results.append({
                "query": query,
                "success": False,
                "error": str(e),
                "rows": 0
            })
    
    # Summary
    print("\n" + "=" * 80)
    print("Batch Summary:")
    print("=" * 80)
    successful = sum(1 for r in results if r["success"])
    print(f"Successful: {successful}/{len(results)}")
    print(f"Failed: {len(results) - successful}/{len(results)}")
    if verbose:
        print("\nDetailed Results:")
        for r in results:
            status = "✅" if r["success"] else "❌"
            print(f"  {status} {r['query']}")
            if not r["success"]:
                print(f"     Error: {r['error']}")
            else:
                print(f"     Rows: {r['rows']}")


def load_queries_from_file(filepath: str) -> List[str]:
    """
    Load queries from a text file (one query per line)
    
    Args:
        filepath: Path to file with queries
        
    Returns:
        List of queries
    """
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            queries = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
        return queries
    except FileNotFoundError:
        print(f"❌ Error: File '{filepath}' not found.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error reading file: {str(e)}")
        sys.exit(1)


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="E-commerce Analytics Agent CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Interactive mode
  python cli.py

  # Single query
  python cli.py "Show me top 10 products by revenue"

  # Multiple queries from file
  python cli.py --file queries.txt

  # Verbose output
  python cli.py --verbose "Segment customers by order value"
        """
    )
    
    parser.add_argument(
        'query',
        nargs='?',
        help='Natural language query to execute'
    )
    
    parser.add_argument(
        '--file', '-f',
        type=str,
        help='File containing queries (one per line)'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Show verbose output including metadata'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        help='Output file for results (JSON format)'
    )
    
    args = parser.parse_args()
    
    # Initialize logging
    setup_logging_config()
    
    # Determine mode
    if args.file:
        # Batch mode from file
        queries = load_queries_from_file(args.file)
        if queries:
            run_batch_queries(queries, verbose=args.verbose)
        else:
            print("❌ No queries found in file.")
            sys.exit(1)
    elif args.query:
        # Single query mode
        run_single_query(args.query, verbose=args.verbose)
    else:
        # Interactive mode
        run_interactive_mode(verbose=args.verbose)


if __name__ == "__main__":
    main()

