import streamlit as st
import pandas as pd
import plotly.express as px
import json
import requests
import spacy
from dotenv import load_dotenv
import os
from datetime import datetime
import hashlib
import pymongo
from pymongo import MongoClient

# Load environment variables
load_dotenv()
API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017/")
MONGODB_DATABASE = os.getenv("MONGODB_DATABASE", "youtube_data")
MONGODB_COLLECTION = os.getenv("MONGODB_COLLECTION", "videos")

# Load NLP model
nlp = spacy.load("en_core_web_sm")

st.set_page_config(page_title="Smart Data Analyzer Pro", layout="wide")
st.title("🧠 Smart Data Analyzer Pro — YouTube Revenue Validation")

# Initialize session state for context memory
if 'conversation_history' not in st.session_state:
    st.session_state.conversation_history = []
if 'analysis_context' not in st.session_state:
    st.session_state.analysis_context = {}
if 'session_id' not in st.session_state:
    st.session_state.session_id = hashlib.md5(str(datetime.now()).encode()).hexdigest()[:8]

def add_to_conversation_history(query, response, insights=None, charts_generated=None):
    """Add query and response to conversation history with metadata"""
    entry = {
        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'query': query,
        'response': response,
        'insights': insights or [],
        'charts_generated': charts_generated or [],
        'session_id': st.session_state.session_id
    }
    st.session_state.conversation_history.append(entry)
    
    # Keep only last 10 conversations to manage memory
    if len(st.session_state.conversation_history) > 10:
        st.session_state.conversation_history = st.session_state.conversation_history[-10:]

def update_analysis_context(key_insights):
    """Update persistent analysis context with key findings"""
    for insight in key_insights:
        category = insight.get('category', 'general')
        if category not in st.session_state.analysis_context:
            st.session_state.analysis_context[category] = []
        st.session_state.analysis_context[category].append({
            'insight': insight.get('text', ''),
            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'confidence': insight.get('confidence', 0.5)
        })

def get_conversation_context():
    """Generate conversation context for AI prompt"""
    if not st.session_state.conversation_history:
        return ""
    
    context = "\n📝 **Previous Conversation Context:**\n"
    for i, entry in enumerate(st.session_state.conversation_history[-5:], 1):  # Last 5 conversations
        context += f"\n**Q{i}:** {entry['query']}\n"
        context += f"**A{i}:** {entry['response'][:200]}{'...' if len(entry['response']) > 200 else ''}\n"
        if entry['insights']:
            context += f"**Key Insights:** {', '.join([insight.get('text', '')[:50] for insight in entry['insights']])}\n"
    
    # Add persistent context
    if st.session_state.analysis_context:
        context += "\n🧠 **Key Analysis Context:**\n"
        for category, insights in st.session_state.analysis_context.items():
            latest_insight = insights[-1] if insights else {}
            context += f"- **{category.title()}:** {latest_insight.get('insight', '')[:100]}\n"
    
    return context

def extract_insights_from_response(response):
    """Extract key insights from AI response for context building"""
    insights = []
    
    # Simple keyword-based insight extraction
    response_lower = response.lower()
    
    if any(word in response_lower for word in ['revenue', 'income', 'earnings']):
        insights.append({'category': 'revenue', 'text': 'Revenue analysis discussed', 'confidence': 0.8})
    
    if any(word in response_lower for word in ['trend', 'growth', 'decline']):
        insights.append({'category': 'trends', 'text': 'Trend analysis provided', 'confidence': 0.7})
    
    if any(word in response_lower for word in ['recommend', 'suggest', 'should']):
        insights.append({'category': 'recommendations', 'text': 'Recommendations provided', 'confidence': 0.9})
    
    if any(word in response_lower for word in ['month', 'seasonal', 'quarterly']):
        insights.append({'category': 'temporal', 'text': 'Temporal analysis conducted', 'confidence': 0.6})
    
    return insights

def connect_to_mongodb():
    """Connect to MongoDB and return the collection"""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]
        
        # Test the connection
        collection.find_one()
        return collection, None
    except Exception as e:
        return None, str(e)

@st.cache_data(ttl=300, hash_funcs={MongoClient: lambda x: None})  # Cache for 5 minutes, ignore MongoDB objects
def load_youtube_data_from_mongodb():
    """Load YouTube data from MongoDB and return as DataFrame"""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]
        
        # Test connection
        collection.find_one()
        
        # Fetch all documents from MongoDB
        cursor = collection.find({})
        data = list(cursor)
        
        # Close the connection
        client.close()
        
        if not data:
            st.warning("No data found in MongoDB collection")
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        
        # Remove MongoDB's _id field if present
        if '_id' in df.columns:
            df = df.drop('_id', axis=1)
        
        # Known record labels for detection
        known_labels = ["T-Series", "Sony Music", "Zee Music", "Tips", "Saregama", "Aditya Music",
                        "Lahari", "Speed Records", "YRF", "Venus", "SVF", "White Hill", "TIPS Official"]
        
        def detect_label(row):
            text = f"{row.get('title', '')} {row.get('description', '')}".lower()
            for label in known_labels:
                if label.lower() in text:
                    return label
            return "Other"
        
        # Add record label detection
        df["Record Label"] = df.apply(detect_label, axis=1)
        
        return df
        
    except Exception as e:
        st.error(f"Error loading data from MongoDB: {str(e)}")
        return pd.DataFrame()

def get_mongodb_stats():
    """Get statistics about the MongoDB collection"""
    try:
        client = MongoClient(MONGODB_URI)
        db = client[MONGODB_DATABASE]
        collection = db[MONGODB_COLLECTION]
        
        stats = {
            "total_documents": collection.count_documents({}),
            "last_updated": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        # Get sample document to show structure
        sample_doc = collection.find_one({})
        if sample_doc:
            stats["sample_fields"] = list(sample_doc.keys())
        
        # Close the connection
        client.close()
        
        return stats
    except Exception as e:
        return {"error": str(e)}

def detect_months_and_confidence(text):
    months = [
        "january", "february", "march", "april", "may", "june",
        "july", "august", "september", "october", "november", "december"
    ]
    found = {token.text for token in nlp(text.lower()) if token.text in months}
    confidence = len(found) / 12
    return list(found), confidence

def render_visuals_from_keywords(text, videos, monthly, top_videos):
    text_lower = text.lower()
    charts_generated = []
    
    months, conf = detect_months_and_confidence(text)
    if conf >= 0.25:
        st.subheader("📈 Monthly Revenue Trend")
        st.caption(f"Confidence: {conf:.2f} — Months detected: {', '.join(months)}")
        fig = px.line(monthly, x="Month", y="Estimated Revenue INR", markers=True)
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(monthly)
        charts_generated.append("Monthly Revenue Trend")

    if "top" in text_lower or "rpv" in text_lower:
        st.subheader("🏆 Top Videos by RPV")
        fig = px.bar(top_videos, x="RPV_Estimated", y="title", orientation="h")
        st.plotly_chart(fig, use_container_width=True)
        st.dataframe(top_videos)
        charts_generated.append("Top Videos by RPV")
    
    return charts_generated

def generate_enhanced_prompt(user_query, label_videos, monthly, est_total, actual_total, rpm):
    label_videos["RPV_Estimated"] = label_videos["Estimated Revenue INR"] / label_videos["view_count"]
    video_sample = label_videos[["title", "view_count", "Estimated Revenue INR", "Month", "RPV_Estimated"]].head(10)
    monthly_sample = monthly.head(10)

    accuracy_str = f"{est_total / actual_total:.2%}" if actual_total else "N/A"
    
    # Get conversation context
    conversation_context = get_conversation_context()

    return f"""
You are a senior Business Intelligence analyst embedded inside a live dashboard with context memory.

{conversation_context}

▶️ **Current Video Data Sample (Top 10):**
{video_sample.to_json(orient="records", indent=2)}

📆 **Monthly Revenue (Top 10):**
{monthly_sample.to_json(orient="records", indent=2)}

📊 **Current Business Summary**:
- RPM: ₹{rpm}
- Total Estimated Revenue: ₹{est_total:,.2f}
- Actual Reported Revenue: ₹{actual_total:,.2f}
- Accuracy: {accuracy_str}
- Total Videos: {len(label_videos)}

---

### Current User Question:
"{user_query}"

---

IMPORTANT INSTRUCTIONS:
1. **Reference Previous Context**: Build upon previous questions and insights when relevant
2. **Provide Continuity**: If this question relates to previous queries, acknowledge and extend the analysis
3. **Use Context**: Reference previous findings, trends, or recommendations when applicable
4. **Avoid Repetition**: Don't repeat identical insights from previous responses
5. **Progressive Analysis**: Deepen the analysis based on conversation history

Respond with data-driven insights only. Use real numbers from the input. Highlight metrics. 
Recommend specific actions. Do not hallucinate. Use markdown formatting with sections and bullet points.

If this is a follow-up question, explicitly connect it to previous analysis and build upon it.
"""

def get_mistral_analysis(prompt, api_key):
    try:
        headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
        payload = {
            "model": "mistralai/mistral-7b-instruct:free",
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7,
            "max_tokens": 1500
        }
        response = requests.post("https://openrouter.ai/api/v1/chat/completions", headers=headers, json=payload, timeout=60)
        if response.status_code == 200:
            return response.json()["choices"][0]["message"]["content"], None
        return "", f"Error {response.status_code}"
    except Exception as e:
        return "", str(e)

# === Sidebar for Context Management and MongoDB Stats ===
with st.sidebar:
    st.header("🧠 Context Memory")
    
    if st.button("🔄 Clear Conversation History"):
        st.session_state.conversation_history = []
        st.session_state.analysis_context = {}
        st.success("Context cleared!")
    
    st.subheader("📊 Session Stats")
    st.metric("Queries Asked", len(st.session_state.conversation_history))
    st.metric("Session ID", st.session_state.session_id)
    
    # MongoDB Connection Status
    st.subheader("🗄️ MongoDB Status")
    mongo_stats = get_mongodb_stats()
    
    if "error" in mongo_stats:
        st.error(f"MongoDB Error: {mongo_stats['error']}")
    else:
        st.success("✅ Connected to MongoDB")
        st.metric("Total Videos", mongo_stats.get("total_documents", 0))
        st.caption(f"Last updated: {mongo_stats.get('last_updated', 'Unknown')}")
        
        if "sample_fields" in mongo_stats:
            with st.expander("📋 Available Fields"):
                for field in mongo_stats["sample_fields"]:
                    st.write(f"• {field}")
    
    # Refresh data button
    if st.button("🔄 Refresh MongoDB Data"):
        st.cache_data.clear()
        st.rerun()
    
    if st.session_state.conversation_history:
        st.subheader("💬 Recent Queries")
        for i, entry in enumerate(st.session_state.conversation_history[-3:], 1):
            with st.expander(f"Query {len(st.session_state.conversation_history) - 3 + i}"):
                st.write(f"**Q:** {entry['query'][:100]}...")
                st.write(f"**Time:** {entry['timestamp']}")
                if entry['charts_generated']:
                    st.write(f"**Charts:** {', '.join(entry['charts_generated'])}")

# === Main App ===
st.info("🗄️ Loading data from MongoDB...")

# Load data from MongoDB instead of JSON
youtube_df = load_youtube_data_from_mongodb()

if youtube_df.empty:
    st.error("⚠️ No data available from MongoDB. Please check your connection and data.")
    st.stop()

st.success(f"✅ Loaded {len(youtube_df)} videos from MongoDB")

uploaded_file = st.file_uploader("📁 Upload Revenue CSV", type=["csv"])

if uploaded_file:
    df = pd.read_csv(uploaded_file)
    
    # Check if we have the required columns
    if "Record Label" not in youtube_df.columns:
        st.error("❌ 'Record Label' column not found in MongoDB data. Please check your data structure.")
        st.stop()
    
    selected_label = st.selectbox("🎙️ Choose Record Label", sorted(youtube_df["Record Label"].unique()))
    rpm = st.number_input("💸 RPM (Revenue per Million Views)", min_value=500, value=1200)

    label_videos = youtube_df[youtube_df["Record Label"] == selected_label].copy()
    label_videos = label_videos.dropna(subset=["view_count"])
    label_videos["Estimated Revenue INR"] = label_videos["view_count"] / 1_000_000 * rpm
    est_total = label_videos["Estimated Revenue INR"].sum()

    if "Store Name" in df.columns:
        yt_row = df[df["Store Name"].str.lower().str.contains("youtube", na=False)]
        actual_total = yt_row["Annual Revenue in INR"].values[0] if not yt_row.empty else 0
    else:
        actual_total = 0

    # Handle different date column names that might come from MongoDB
    date_columns = ['published_at', 'publishedAt', 'upload_date', 'date', 'created_at']
    date_column = None
    
    for col in date_columns:
        if col in label_videos.columns:
            date_column = col
            break
    
    if date_column:
        label_videos[date_column] = pd.to_datetime(label_videos[date_column], errors="coerce")
        label_videos["Month"] = label_videos[date_column].dt.to_period("M").astype(str)
    else:
        st.warning("⚠️ No date column found. Monthly analysis will be limited.")
        label_videos["Month"] = "Unknown"
    
    monthly_revenue = label_videos.groupby("Month")["Estimated Revenue INR"].sum().reset_index()
    label_videos["RPV_Estimated"] = label_videos["Estimated Revenue INR"] / label_videos["view_count"]
    top_rpv = label_videos.nlargest(10, "RPV_Estimated")[["title", "view_count", "Estimated Revenue INR", "RPV_Estimated"]]

    # Display metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Estimated Revenue", f"₹{est_total:,.0f}")
    with col2:
        st.metric("Actual Revenue", f"₹{actual_total:,.0f}")
    with col3:
        st.metric("Accuracy", f"{(est_total / actual_total):.2%}" if actual_total else "N/A")

    # Enhanced query interface
    st.subheader("🧠 Ask a Business Intelligence Question")
    
    # Show context hint
    if st.session_state.conversation_history:
        last_query = st.session_state.conversation_history[-1]['query']
        st.info(f"💡 **Context Available** - Last query: '{last_query[:50]}...' - Ask follow-up questions for deeper insights!")
    
    user_query = st.text_area("Your question:", placeholder="Ask about trends, comparisons, recommendations, or follow up on previous insights...")
    
    # Quick suggestion buttons based on context
    if st.session_state.conversation_history:
        st.write("**Quick Follow-ups:**")
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📈 Deep dive into this"):
                user_query = f"Can you provide a deeper analysis of the previous insight?"
        with col2:
            if st.button("🔍 What's next?"):
                user_query = f"Based on the previous analysis, what should be the next steps?"
        with col3:
            if st.button("📊 Compare trends"):
                user_query = f"How do these findings compare to industry benchmarks?"
    
    if user_query:
        with st.spinner("🧠 Mistral AI is analyzing with context..."):
            full_prompt = generate_enhanced_prompt(user_query, label_videos, monthly_revenue, est_total, actual_total, rpm)
            response, error = get_mistral_analysis(full_prompt, API_KEY)
            
            if error:
                st.error(error)
            else:
                st.markdown("### 🧠 Mistral Insight")
                st.markdown(response)
                
                # Generate visuals and track them
                charts_generated = render_visuals_from_keywords(response, label_videos, monthly_revenue, top_rpv)
                
                # Extract insights and update context
                insights = extract_insights_from_response(response)
                update_analysis_context(insights)
                
                # Add to conversation history
                add_to_conversation_history(user_query, response, insights, charts_generated)
                
                # Show connection to previous context
                if len(st.session_state.conversation_history) > 1:
                    with st.expander("🔗 Context Connection"):
                        st.write("This analysis builds upon:")
                        for insight in insights:
                            st.write(f"- {insight['category'].title()}: {insight['text']}")

    st.download_button("📥 Export Video Data", label_videos.to_csv(index=False), "videos.csv")
    
    # Export conversation history
    if st.session_state.conversation_history:
        conversation_export = pd.DataFrame(st.session_state.conversation_history)
        st.download_button(
            "📥 Export Conversation History", 
            conversation_export.to_csv(index=False), 
            f"conversation_history_{st.session_state.session_id}.csv"
        )

else:
    st.info("📁 Upload a revenue CSV to get started.")
    
    # Show welcome message with context capabilities and MongoDB info
    st.markdown("""
    ### 🧠 Context Memory Features:
    - **Conversation History**: Remembers your previous questions and insights
    - **Progressive Analysis**: Each query builds upon previous findings
    - **Context Awareness**: AI understands the flow of your analysis
    - **Session Persistence**: Maintains context throughout your session
    - **Export Conversations**: Download your analysis journey
    
    ### 🗄️ MongoDB Integration:
    - **Real-time Data**: Fetches fresh data from your MongoDB collection
    - **Automatic Refresh**: Data updates every 5 minutes
    - **Connection Monitoring**: Shows MongoDB connection status
    - **Field Detection**: Automatically detects available data fields
    """)