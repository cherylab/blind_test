mkdir -p ~/.streamlit/

echo "\
[general]\n\
email = \"cherylab92@gmail.com\"\n\
" > ~/.streamlit/credentials.toml

#echo "[server]" >> ~/.streamlit/config.toml
#echo "headless = true" >> ~/.streamlit/config.toml
#echo "port = $PORT" >> ~/.streamlit/config.toml
#echo "enableCORS = false" >> ~/.streamlit/config.toml

echo "[theme]
primaryColor='#2987C8'
backgroundColor='#ffffff'
secondaryBackgroundColor='#f1f1f1'
textColor='#4c4c4c'
font = 'sans serif'
[server]\n\
headless = true\n\
enableCORS=false\n\
port = $PORT\n\
" > ~/.streamlit/config.toml