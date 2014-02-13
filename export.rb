require 'open-uri'
require 'json'
require 'csv'
require 'pathname'
require 'set'

FOLDER = Pathname.new('./data/')

def load_addresses(addresses, n)
    while n >= 0 do
        File.write(FOLDER + ('address_'+n.to_s+'.json'), open('http://blockchain.info/multiaddr?active=' + addresses.join('%7C') +'&format=json&limit=50&offset=' + n.to_s).read)
        sleep(0.2)
        n = n - 50
    end
end

def get_input_data(trans, addr = 'addr')
    data = [[], []]
    trans['inputs'].each do |input|
        data.first << input['prev_out'][addr]
        data.last << input['prev_out']['value']
    end
    data
end

def get_out_data(trans, addr = 'addr')
    data = [[], []]
    trans['out'].each do |output|
        data.first << output[addr]
        data.last << output['value']
    end
    data
end

def gather_transactions(addresses)
    transactions = []
    Dir[FOLDER + 'address_*.json'].each do |filename|
        transactions += JSON.load(File.read(filename))['txs']
    end
    transactions.uniq!
    transactions.sort_by! { |t| t['time'] }
    transactions.each do |trans|
        input_addrs = get_input_data(trans).first
        hash = trans['hash']
        filename = FOLDER + ('tx_'+hash+'.json')
        next if filename.exist?
        File.write(filename, open('http://blockchain.info/rawtx/' + hash).read)
        sleep(0.3)
    end
end

def load_transactions
    transactions = []
    Dir[FOLDER + 'tx_*.json'].each do |filename|
        transactions << JSON.load(File.read(filename))
    end
    transactions.uniq!
    transactions.sort_by! { |t| t['time'] }
    transactions
end

def consolidate(participants)
    newparticipants = []
    participants.each_index do |i|
        p = participants[i]
        (i + 1).upto(participants.count - 1) do |j|
            current = participants[j]
            p += current unless (p & current).empty?
        end
        newparticipants << p
    end
    newparticipants
end

def consolidateAll(participants)
    begin
        start = participants.count
        participants = consolidate(participants)
    end until participants.count == start
    participants
end

def get_participant(current, participants)
    participants.each_index do |i|
        unless (participants[i] & current).empty?
            participants[i] += current
            return participants[i]
        end
    end
    participants << current.to_set
    participants.last
end

def getAllParticipants(transactions)
    participants = []
    transactions.each do |trans|
        input_data = get_input_data(trans)
        out_data = get_out_data(trans)
        input_addrs = input_data.first
        out_addrs = out_data.first
        get_participant(input_addrs, participants)
    end
    consolidateAll(participants)
end

def filter_transactions(transactions, addresses)
    transactionList = []
    participants = getAllParticipants(transactions)
    transactions.each do |trans|
        input_data = get_input_data(trans)
        out_data = get_out_data(trans)
        input_addrs = input_data.first
        out_addrs = out_data.first
        inputAddrs = get_participant(input_addrs, participants)
        inputAddr = inputAddrs.first
        transactionData = {}
        transactionData['Time'] = trans['time']
        transactionData['TXID'] = trans['hash']
        transactionData['Type'] = nil
        out_addrs.each_index do |i|
            if (input_addrs & addresses).empty? and addresses.include?(out_addrs[i])
                transactionData['Type'] = 'SEND'
                transactionData['Address'] = inputAddr
                transactionData['Amount'] ||= 0
                transactionData['Amount'] -= out_data.last[i]
            elsif not (input_addrs & addresses).empty? and not addresses.include?(out_addrs[i])
                outputAddrs = get_participant([out_addrs[i]], participants)
                outputAddr = outputAddrs.first
                transactionData['Type'] = 'RECEIVE'
                transactionData['Address'] = outputAddr
                transactionData['Amount'] ||= 0
                transactionData['Amount'] += out_data.last[i]
            end
        end
        transactionList << transactionData if transactionData['Type']
    end
    transactionList.sort_by { |t| t['Time'] }
end

def save_csv(transactionList)
    CSV.open('output.csv', 'wb', {:col_sep => ';', :headers => true}) do |csv|
        csv << ['Time','TXID', 'Address','Type', 'Amount']
        transactionList.each do |trans|
            csv << [trans['Time'], trans['TXID'], trans['Address'], trans['Type'], trans['Amount']]
        end
    end
end

n = 100
addresses = [] # Fill with target Addresses

#load_addresses(addresses, n)
#gather_transactions(addresses)
#transactions = load_transactions
#transactionList = filter_transactions(transactions, addresses)
#save_csv(transactionList)

