#!/bin/sh

alias nessie='java -jar /nessie-cli.jar -u http://nessie:19120/api/v2'

echo "Bây giờ bạn có thể sử dụng, ví dụ:"
echo "  nessie -c \"branch -l\""
echo "  nessie log main"
echo "  nessie                (để vào chế độ REPL)"



