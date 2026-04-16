"""
Inventory Viewer — Input Mapping Version
=========================================
A Keboola Data App that displays inventory data from input mapping.
Data is loaded from CSV files at /data/in/tables/ (Keboola's standard input path).

This is a READ-ONLY version — no Query Service, no writes to Storage.
Configure input mapping in your Data App to load the inventory table.
"""

import os
import glob
from flask import Flask, render_template, jsonify
import pandas as pd

app = Flask(__name__)

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

# Keboola input mapping paths
DATA_DIR = os.environ.get('KBC_DATADIR', '/data')
INPUT_TABLES_DIR = os.path.join(DATA_DIR, 'in', 'tables')

# Expected table name (can be configured via env var)
INVENTORY_TABLE_NAME = os.environ.get('INVENTORY_TABLE', 'inventory')


# -----------------------------------------------------------------------------
# Data Loading from Input Mapping
# -----------------------------------------------------------------------------

def find_inventory_file():
    """Find the inventory CSV file in input mapping directory."""
    # Try exact match first
    exact_path = os.path.join(INPUT_TABLES_DIR, f'{INVENTORY_TABLE_NAME}.csv')
    if os.path.exists(exact_path):
        return exact_path
    
    # Try with Keboola's table ID format (e.g., in.c-demo.inventory.csv)
    pattern = os.path.join(INPUT_TABLES_DIR, f'*{INVENTORY_TABLE_NAME}*.csv')
    matches = glob.glob(pattern)
    if matches:
        return matches[0]
    
    # List all available CSVs for debugging
    all_csvs = glob.glob(os.path.join(INPUT_TABLES_DIR, '*.csv'))
    if all_csvs:
        # Return first CSV as fallback
        return all_csvs[0]
    
    return None


def load_inventory():
    """Load inventory data from input mapping CSV."""
    csv_path = find_inventory_file()
    
    if not csv_path or not os.path.exists(csv_path):
        return pd.DataFrame(), f"No inventory file found in {INPUT_TABLES_DIR}"
    
    try:
        df = pd.read_csv(csv_path)
        # Normalize column names to lowercase
        df.columns = df.columns.str.lower()
        return df, None
    except Exception as e:
        return pd.DataFrame(), f"Error reading {csv_path}: {str(e)}"


def get_products(search=None, category=None):
    """Get products with optional filtering."""
    df, error = load_inventory()
    
    if error:
        return [], error
    
    if df.empty:
        return [], None
    
    # Apply filters
    if search:
        search_lower = search.lower()
        mask = (
            df['name'].str.lower().str.contains(search_lower, na=False) |
            df['id'].astype(str).str.lower().str.contains(search_lower, na=False)
        )
        df = df[mask]
    
    if category and category != 'all':
        df = df[df['category'] == category]
    
    # Sort by name
    df = df.sort_values('name', ignore_index=True)
    
    return df.to_dict('records'), None


def get_categories():
    """Get distinct categories."""
    df, error = load_inventory()
    
    if error or df.empty:
        return []
    
    return sorted(df['category'].dropna().unique().tolist())


def get_stats():
    """Get inventory statistics."""
    df, error = load_inventory()
    
    if error or df.empty:
        return {'total': 0, 'categories': 0, 'low_stock': 0, 'total_value': 0}
    
    return {
        'total': len(df),
        'categories': df['category'].nunique(),
        'low_stock': len(df[df['quantity'] <= 5]) if 'quantity' in df.columns else 0,
        'total_value': (df['quantity'] * df['price']).sum() if 'quantity' in df.columns and 'price' in df.columns else 0
    }


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------

@app.route('/', methods=['GET', 'POST'])
def index():
    """Main page — must handle POST (Keboola startup check)."""
    return render_template('index.html')


@app.route('/api/products')
def api_products():
    """API: Get products with optional filtering."""
    from flask import request
    
    search = request.args.get('search', '').strip()
    category = request.args.get('category', 'all')
    
    products, error = get_products(search, category)
    
    if error:
        return jsonify({'success': False, 'error': error}), 500
    
    return jsonify({
        'success': True,
        'products': products,
        'total': len(products)
    })


@app.route('/api/categories')
def api_categories():
    """API: Get distinct categories."""
    categories = get_categories()
    return jsonify({'success': True, 'categories': categories})


@app.route('/api/stats')
def api_stats():
    """API: Get inventory statistics."""
    stats = get_stats()
    return jsonify({'success': True, 'stats': stats})


@app.route('/api/health')
def health():
    """Health check endpoint."""
    csv_path = find_inventory_file()
    df, error = load_inventory()
    
    return jsonify({
        'status': 'healthy' if not error else 'degraded',
        'input_dir': INPUT_TABLES_DIR,
        'inventory_file': csv_path,
        'row_count': len(df) if not error else 0,
        'error': error
    })


@app.route('/api/debug')
def debug():
    """Debug endpoint — list available input files."""
    files = []
    if os.path.exists(INPUT_TABLES_DIR):
        for f in os.listdir(INPUT_TABLES_DIR):
            fpath = os.path.join(INPUT_TABLES_DIR, f)
            files.append({
                'name': f,
                'size': os.path.getsize(fpath) if os.path.isfile(fpath) else 0,
                'is_file': os.path.isfile(fpath)
            })
    
    return jsonify({
        'data_dir': DATA_DIR,
        'input_tables_dir': INPUT_TABLES_DIR,
        'exists': os.path.exists(INPUT_TABLES_DIR),
        'files': files
    })


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8050))
    app.run(host='0.0.0.0', port=port, debug=False)
