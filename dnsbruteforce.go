package main

import (
    "bufio"
    "fmt"
    "flag"
    "github.com/miekg/dns"
    "os"
    "sync"
    "strings"
)

type DNSResponse struct {
    query string
    answer []string
    err error
}


func makeDNSResponse(query string, response *dns.Msg, err error) (*DNSResponse){
    var ans []string
    if response != nil {
        for _, answer := range response.Answer{
            switch answer.(type) {
                case *dns.A:
                    ans = append(ans, answer.(*dns.A).A.String())
                case *dns.CNAME:
                    ans = append(ans, answer.(*dns.CNAME).Target)
            }   
        }
    }
    resp := DNSResponse{query:query, answer:ans, err:err}
    return &resp
}

func readDictionary(done <- chan struct{}, dictionaryName string, base string) (<- chan string) {
    output := make(chan string)
    go func() {
        file, err := os.Open(dictionaryName)
        if err != nil {
            fmt.Println("Error reading dictionary")
            return 
        }
        defer file.Close()
        defer close(output)
        scanner := bufio.NewScanner(file)        
        for scanner.Scan() {
            name := strings.ToLower(scanner.Text()) + "." + strings.ToLower(base) + "."
            select {
                case output <- name:
                case <-done:
                    return
            }
        }
    }()
    return output
}


func digester(server string, done <-chan struct{}, names <-chan string, output chan<- DNSResponse){
    for targetName := range names {
        message := new(dns.Msg)
        client := new(dns.Client)
        client.Timeout = 10 * 1e9
        message.SetQuestion(targetName, dns.TypeA)
        response, _, err := client.Exchange(message, server + ":53")
        // retry errors.
        if err != nil {
            response, _, err = client.Exchange(message, server + ":53")
        }
        resp := makeDNSResponse(targetName, response, err)
        select {
            case output <- *resp:
            case <-done:
                return       
        }
    }
}


func LookupAll(base string, dictionary string, server string, workers int) ([]DNSResponse, error){
    done := make(chan struct{})
    defer close(done)

    names := readDictionary(done, dictionary, base)
    
    responseChan := make(chan DNSResponse)
    
    var wg sync.WaitGroup
    wg.Add(workers)
    for i:= 0; i < workers; i++ {
        go func() {
            digester(server, done, names, responseChan)
            wg.Done()
        }()
    
    }
    
    go func() {
        wg.Wait()
        close(responseChan)
    
    }()
    
    var answers []DNSResponse
    for r := range responseChan {
        if r.err != nil{
            fmt.Println("error looking up", r.query, r.err)
        }
        if len(r.answer) > 0 {
            answers = append(answers, r)
        }
    }
    return answers, nil
}


func main(){
    name := flag.String("domain", "google.com", "domain to brute force")
    dictionary := flag.String("dict", "dictionary.txt" , "dictionary to load")
    server := flag.String("server", "8.8.8.8", "Resolver to use")
    numWorkers := flag.Int("workers", 10, "Number of Workers")
    
    flag.Parse()
    
    responses, err := LookupAll(*name, *dictionary, *server, *numWorkers)
    if err != nil{
        fmt.Println(err)
        return
    }
    for _, entry := range responses{
        fmt.Println(entry.query + " - " +  strings.Join(entry.answer, ", "))
    }
     
}
