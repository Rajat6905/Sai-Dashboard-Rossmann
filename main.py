import os
import uvicorn
if __name__ == "__main__":
    hostip = "0.0.0.0"
    # hostip = constants.SERVER_IP
    uvicorn.run("app:app",
                host=hostip, port=443, reload=True, workers=5)
