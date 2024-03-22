if [ -f ./input.csv ]; then
    scrapy crawl miamidade
    if [ -f ./miamidade.json ]; then
        scrapy crawl truepeoplesearch
    else
        echo "miamidade.json file is missing"
    fi
else
    echo "Input.csv file is missing"
fi


