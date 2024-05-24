def butterfingers(text,prob=0.1,keyboard='qwerty'):
    import random

    """ taken from
        https://github.com/Decagon/butter-fingers/blob/master/butterfingers/butterfingers.py """

    keyApprox = {}

    if keyboard == "qwerty":
        keyApprox['q'] = "qwasedzx"
        keyApprox['w'] = "wqesadrfcx"
        keyApprox['e'] = "ewrsfdqazxcvgt"
        keyApprox['r'] = "retdgfwsxcvgt"
        keyApprox['t'] = "tryfhgedcvbnju"
        keyApprox['y'] = "ytugjhrfvbnji"
        keyApprox['u'] = "uyihkjtgbnmlo"
        keyApprox['i'] = "iuojlkyhnmlp"
        keyApprox['o'] = "oipklujm"
        keyApprox['p'] = "plo['ik"

        keyApprox['a'] = "aqszwxwdce"
        keyApprox['s'] = "swxadrfv"
        keyApprox['d'] = "decsfaqgbv"
        keyApprox['f'] = "fdgrvwsxyhn"
        keyApprox['g'] = "gtbfhedcyjn"
        keyApprox['h'] = "hyngjfrvkim"
        keyApprox['j'] = "jhknugtblom"
        keyApprox['k'] = "kjlinyhn"
        keyApprox['l'] = "lokmpujn"

        keyApprox['z'] = "zaxsvde"
        keyApprox['x'] = "xzcsdbvfrewq"
        keyApprox['c'] = "cxvdfzswergb"
        keyApprox['v'] = "vcfbgxdertyn"
        keyApprox['b'] = "bvnghcftyun"
        keyApprox['n'] = "nbmhjvgtuik"
        keyApprox['m'] = "mnkjloik"
        keyApprox[' '] = " "
    else:
        print("Keyboard not supported.")

    probOfTypo = int(prob * 100)

    buttertext = ""
    for letter in text:
        lcletter = letter.lower()
        if not lcletter in keyApprox.keys():
            newletter = lcletter
        else:
            if random.choice(range(0, 100)) <= probOfTypo:
                    newletter = random.choice(keyApprox[lcletter])
            else:
                    newletter = lcletter
        # go back to original case
        if not lcletter == letter:
            newletter = newletter.upper()
        buttertext += newletter

    return buttertext


