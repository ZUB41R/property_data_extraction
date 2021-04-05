docker run --rm --name z-property-scraper --env-file $(pwd)/utils/args.env -v $(pwd)/deploy/keepitsecret:/app/keepitsecret -v $(pwd)/resources:/app/resources --net=host web_scraping/property_scraper:recent_build

# docker run -it --name z-property-scraper --env-file /Users/zubairahmed/PROJECTS/web_scraping/property_scraper/utils/args.env -v /Users/zubairahmed/PROJECTS/web_scraping/property_scraper/keepitsecret:/app/keepitsecret --net=host web_scraping/property_scraper:recent_build bash
