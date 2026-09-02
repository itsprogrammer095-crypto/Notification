# Render.com par Server Host karna

## Step 1: GitHub par code daalo
1. GitHub par naya banao (private repo bhi chalega), naam jaise `device-monitor`
2. In 2 files ko `server/` folder me daalo:
   - `server.py`
   - `requirements.txt`
3. Push karo:
   ```
   cd D:/Notification_call_log
   git init
   git add server/server.py server/requirements.txt
   git commit -m "deploy server"
   git branch -M main
   git remote add origin https://github.com/USERNAME/device-monitor.git
   git push -u origin main
   ```

## Step 2: Render par Web Service banao
1. https://dashboard.render.com par login karo (GitHub se sign-in)
2. **New +** → **Web Service**
3. Apna GitHub repo connect karo
4. Ye settings bharo:
   - **Name**: `device-monitor` (jo bhi chaho)
   - **Region**: Singapore (India ke liye fastest)
   - **Branch**: `main`
   - **Root Directory**: `server`  ← important
   - **Runtime**: Python 3
   - **Build Command**: `pip install -r requirements.txt`
   - **Start Command**: `python server.py`
   - **Instance Type**: Free
5. **Create Web Service** dabao
6. 2-3 min me live ho jayega. URL milega jaise:
   `https://device-monitor-xxxx.onrender.com`

## Step 3: Phone ka APK update karo
Render ka URL APK me daalna hoga:
- `ApiClient.kt` me `DEFAULT_SERVER` change karo:
  ```kotlin
  const val DEFAULT_SERVER = "https://device-monitor-xxxx.onrender.com"
  ```
- APK rebuild karke phone me install karo

## ⚠️ Important cheezein

**1. Free tier par server sota hai (spin down)**
15 min tak koi request na aaye to Render server band kar deta hai.
Phir pehli request par 30-60 sec jaagta hai.
Phone ka heartbeat har 2-3 sec me aata hai, isliye **server kabhi slega nahi** —
jab tak phone ka app chalu hai. Ye tumhare liye automatic "always on" hai.

**2. Data (SQLite) restart par mit jayega**
Free tier par disk temporary hai. Server redeploy/restart par `monitor_data.db` ka
data ud jayega. Calls/notifs dobara sync ho jayenge (phone se full sync aata hai),
par purana dashboard history chali jayegi. Permanent chahiye to Render ka paid
**Persistent Disk** ($1-2/month) lagega.

**3. Heartbeat long-poll timeout**
Render free tier 100 sec ka request timeout lagata hai. Hamara heartbeat max
25 sec ka long-poll hai, isliye koi dikkat nahi.

## Local testing (Render deploy se pehle)
```
cd D:/Notification_call_log/server
set PORT=5000
python server.py
```
Ab `http://localhost:5000` par wahi chalega jo Render par chalega.
